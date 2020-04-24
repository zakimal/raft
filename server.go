package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu sync.Mutex

	serverID int
	peerIDs []int

	cm *ConsensusModule
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit chan interface{}
	wg sync.WaitGroup
}

func NewServer(serverID int, peerIDs []int, ready <-chan interface{}) *Server {
	s := new(Server)
	s.serverID = serverID
	s.peerIDs = peerIDs
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})

	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverID, s.peerIDs, s, s.ready)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{cm: s.cm}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverID, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectTo(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerID] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerID] = client
	}
	return nil
}

func (s *Server) Disconnect(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerID] != nil {
		err := s.peerClients[peerID].Close()
		s.peerClients[peerID] = nil
		return err
	}
	return nil
}

func (s *Server) Call(peerID int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[peerID]

	if peer == nil {
		return fmt.Errorf("call client %d after its connection has closed", peerID)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (proxy *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if 0 < len(os.Getenv("RAFT_UNRELIABLE_RPC")) {
		dice := rand.Intn(10)
		if dice == 9 {
			proxy.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			proxy.cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1 + rand.Intn(5)) * time.Millisecond)
	}

	return proxy.cm.RequestVote(args, reply)
}

func (proxy *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if 0 < len(os.Getenv("RAFT_UNRELIABLE_RPC")) {
		dice := rand.Intn(10)
		if dice == 9 {
			proxy.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			proxy.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1 + rand.Intn(5)) * time.Millisecond)
	}

	return proxy.cm.AppendEntries(args, reply)
}