package node

import (
	"context"
	"fmt"
	"github.com/nathanieltornow/mercurius"
	"github.com/nathanieltornow/mercurius/node/nodepb"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
)

type Node struct {
	nodepb.UnimplementedNodeServer

	id    uint32
	epoch uint32
	sn    uint32

	numOfPeers        uint32
	acknowledgments   map[uint64]uint32
	acknowledgmentsMu sync.Mutex

	app mercurius.Application

	ctr           uint32
	clientStreams map[uint32]nodepb.Node_PrepareAndAcknowledgeClient
	serverStreams map[uint32]nodepb.Node_PrepareAndAcknowledgeServer
	streamsMu     sync.RWMutex

	comReqCh chan *mercurius.CommitRequest
}

func NewNode(id uint32, app mercurius.Application) *Node {
	n := new(Node)
	n.id = id
	n.app = app
	n.acknowledgments = make(map[uint64]uint32)
	n.clientStreams = make(map[uint32]nodepb.Node_PrepareAndAcknowledgeClient)
	n.serverStreams = make(map[uint32]nodepb.Node_PrepareAndAcknowledgeServer)
	n.comReqCh = make(chan *mercurius.CommitRequest, 2048)
	return n
}

func (n *Node) Start(IP string, peerIPs []string) error {
	errCh := make(chan error, 1)
	n.streamsMu.Lock()
	for _, peerIP := range peerIPs {
		conn, err := grpc.Dial(peerIP, grpc.WithInsecure())
		if err != nil {
			return err
		}
		nodeClient := nodepb.NewNodeClient(conn)
		stream, err := nodeClient.PrepareAndAcknowledge(context.Background())
		if err != nil {
			return err
		}
		n.clientStreams[n.ctr] = stream
		n.ctr++
		go func() {
			err := n.clientPrepareAndAcknowledge(stream)
			if err != nil {
				errCh <- err
			}
		}()
	}
	n.streamsMu.Unlock()

	n.acknowledgmentsMu.Lock()
	n.numOfPeers = uint32(len(peerIPs))
	n.acknowledgmentsMu.Unlock()

	go func() {
		err := n.handleAppCommitRequests()
		if err != nil {
			errCh <- err
		}
	}()

	lis, err := net.Listen("tcp", IP)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	nodepb.RegisterNodeServer(grpcServer, n)
	if err := grpcServer.Serve(lis); err != nil {
		errCh <- err
	}
	return <-errCh
}

func (n *Node) MakeCommitRequest(comReq *mercurius.CommitRequest) {
	n.comReqCh <- comReq
}

func (n *Node) PrepareAndAcknowledge(stream nodepb.Node_PrepareAndAcknowledgeServer) error {
	n.streamsMu.Lock()
	n.serverStreams[n.ctr] = stream
	n.ctr++
	n.streamsMu.Unlock()

	n.acknowledgmentsMu.Lock()
	n.numOfPeers++
	n.acknowledgmentsMu.Unlock()

	for {
		in, err := stream.Recv()
		if err != nil {
			return err
		}
		switch in.MessageType {
		case nodepb.MessageType_ACK:
			ack := false
			n.acknowledgmentsMu.Lock()
			n.acknowledgments[in.SN]++
			if n.acknowledgments[in.SN] == atomic.LoadUint32(&n.numOfPeers) {
				delete(n.acknowledgments, in.SN)
				ack = true
			}
			n.acknowledgmentsMu.Unlock()
			if ack {
				if err := n.app.Commit(in.SN, in.Key); err != nil {
					return fmt.Errorf("app failed to commit: %v\n", err)
				}
				if err := n.app.Acknowledge(in.SN, in.Key); err != nil {
					return fmt.Errorf("app failed to acknowledge: %v\n", err)
				}
			}
		case nodepb.MessageType_PREP:
			if err := n.app.Prepare(in.SN, in.Key, in.Content); err != nil {
				return fmt.Errorf("app failed to prepare: %v\n", err)
			}
			if err := n.app.Commit(in.SN, in.Key); err != nil {
				return fmt.Errorf("app failed to commit: %v\n", err)
			}
			if err := stream.Send(&nodepb.PeerMessage{MessageType: nodepb.MessageType_ACK, SN: in.SN, Key: in.Key}); err != nil {
				return err
			}
		}
	}
}

func (n *Node) clientPrepareAndAcknowledge(stream nodepb.Node_PrepareAndAcknowledgeClient) error {
	for {
		in, err := stream.Recv()
		if err != nil {
			return err
		}
		switch in.MessageType {
		case nodepb.MessageType_ACK:
			ack := false
			n.acknowledgmentsMu.Lock()
			n.acknowledgments[in.SN]++
			if n.acknowledgments[in.SN] == atomic.LoadUint32(&n.numOfPeers) {
				delete(n.acknowledgments, in.SN)
				ack = true
			}
			n.acknowledgmentsMu.Unlock()
			if ack {
				if err := n.app.Commit(in.SN, in.Key); err != nil {
					return fmt.Errorf("app failed to commit: %v\n", err)
				}
				if err := n.app.Acknowledge(in.SN, in.Key); err != nil {
					return fmt.Errorf("app failed to acknowledge: %v\n", err)
				}
			}
		case nodepb.MessageType_PREP:
			if err := n.app.Prepare(in.SN, in.Key, in.Content); err != nil {
				return fmt.Errorf("app failed to prepare: %v\n", err)
			}
			if err := n.app.Commit(in.SN, in.Key); err != nil {
				return fmt.Errorf("app failed to commit: %v\n", err)
			}
			if err := stream.Send(&nodepb.PeerMessage{MessageType: nodepb.MessageType_ACK, SN: in.SN, Key: in.Key}); err != nil {
				return err
			}
		}
	}
}

func (n *Node) handleAppCommitRequests() error {
	for comReq := range n.comReqCh {
		sn := n.nextSN()
		if err := n.app.Prepare(sn, comReq.Key, comReq.Content); err != nil {
			return fmt.Errorf("app failed to prepare: %v\n", err)
		}
		prepMsg := &nodepb.PeerMessage{MessageType: nodepb.MessageType_PREP, Content: comReq.Content, SN: sn, Key: comReq.Key}
		if err := n.broadcastPeerMessage(prepMsg); err != nil {
			return fmt.Errorf("failed to broadcast prep-msg: %v\n", err)
		}
	}
	return nil
}

func (n *Node) broadcastPeerMessage(peerMsg *nodepb.PeerMessage) error {
	n.streamsMu.RLock()
	for _, stream := range n.clientStreams {
		if err := stream.Send(peerMsg); err != nil {
			return err
		}
	}
	for _, stream := range n.serverStreams {
		if err := stream.Send(peerMsg); err != nil {
			return err
		}
	}
	n.streamsMu.RUnlock()
	return nil
}

func (n *Node) nextSN() uint64 {
	return (uint64(atomic.LoadUint32(&n.epoch)) << 32) + uint64(atomic.AddUint32(&n.sn, 1))
}
