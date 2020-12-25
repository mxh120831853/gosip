package transport

import (
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/ghettovoice/gosip/log"
	"github.com/ghettovoice/gosip/sip"
)

type wsProtocol struct {
	protocol
	listeners   ListenerPool
	connections ConnectionPool
	conns       chan Connection
}

func NewWsProtocol(
	output chan<- sip.Message,
	errs chan<- error,
	cancel <-chan struct{},
	msgMapper sip.MessageMapper,
	logger log.Logger,
) Protocol {
	wsp := new(wsProtocol)
	wsp.network = "ws"
	wsp.reliable = true
	wsp.streamed = true
	wsp.conns = make(chan Connection)
	wsp.log = logger.
		WithPrefix("transport.Protocol").
		WithFields(log.Fields{
			"protocol_ptr": fmt.Sprintf("%p", wsp),
		})
	// TODO: add separate errs chan to listen errors from pool for reconnection?
	wsp.listeners = NewListenerPool(wsp.conns, errs, cancel, wsp.Log())
	wsp.connections = NewConnectionPool(output, errs, cancel, msgMapper, wsp.Log())
	// pipe listener and connection pools
	go wsp.pipePools()

	return wsp
}

func (wsp *wsProtocol) pipePools() {
	defer close(wsp.conns)

	wsp.Log().Debug("start pipe pools")
	defer wsp.Log().Debug("stop pipe pools")

	wsUp := ws.Upgrader{
		Protocol: func(val []byte) bool {
			return string(val) == "sip"
		},
	}

	for {
		select {
		case <-wsp.listeners.Done():
			return
		case conn := <-wsp.conns:
			conn.SetKey(ConnectionKey(strings.Replace(string(conn.Key()), "tcp", "ws", 1)))

			logger := log.AddFieldsFrom(wsp.Log(), conn)

			if _, err := wsUp.Upgrade(conn); err != nil {
				logger.Errorf("websocket connection upgrade failed: %s", err)

				conn.Close()

				continue
			}

			if err := wsp.connections.Put(wrapWsConn(conn), sockTTL); err != nil {
				// TODO should it be passed up to UA?
				logger.Errorf("put new TCP connection failed: %s", err)

				conn.Close()

				continue
			}
		}
	}
}

type wsConn struct {
	Connection
	r    *wsutil.Reader
	w    *wsutil.Writer
	ctrl wsutil.FrameHandlerFunc
}

func wrapWsConn(conn Connection) *wsConn {
	wsc := &wsConn{
		Connection: conn,
		ctrl:       wsutil.ControlFrameHandler(conn, ws.StateServerSide),
		r:          wsutil.NewServerSideReader(conn),
		w:          wsutil.NewWriter(conn, ws.StateServerSide, ws.OpText),
	}
	controlHandler := wsc.ctrl
	wsc.r.CheckUTF8 = true
	wsc.r.OnIntermediate = controlHandler
	return wsc
}

func (conn *wsConn) Read(buf []byte) (int, error) {
	for {
		hdr, err := conn.r.NextFrame()
		if err != nil {
			return 0, fmt.Errorf("read ws next frame: %w", err)
		}
		if hdr.OpCode == ws.OpClose {
			return 0, io.EOF
		}
		if hdr.OpCode.IsControl() {
			if err := conn.ctrl(hdr, conn.r); err != nil {
				return 0, fmt.Errorf("handle ws control message: %w", err)
			}
			continue
		}
		if hdr.OpCode&ws.OpText == 0 {
			if err := conn.r.Discard(); err != nil {
				return 0, fmt.Errorf("discard ws non-text message: %w", err)
			}
			continue
		}
		if n, err := io.ReadFull(conn.r, buf[:hdr.Length]); err == nil {
			return n, nil
		} else {
			return n, fmt.Errorf("read ws message payload: %w", err)
		}
	}
}

func (conn *wsConn) Write(buf []byte) (int, error) {
	if n, err := conn.w.Write(buf); err == nil {
		if err = conn.w.Flush(); err != nil {
			err = fmt.Errorf("flush ws writer: %w", err)
		}
		return n, err
	} else {
		return n, fmt.Errorf("write to ws connection: %w", err)
	}
}

func (wsp *wsProtocol) Done() <-chan struct{} {
	return wsp.connections.Done()
}

func (wsp *wsProtocol) Listen(target *Target) error {
	target = FillTargetHostAndPort(wsp.Network(), target)
	// resolve local TCP endpoint
	laddr, err := wsp.resolveTarget(target)
	if err != nil {
		return err
	}
	// create listener
	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return &ProtocolError{
			err,
			fmt.Sprintf("listen on %s %s address", wsp.Network(), laddr),
			fmt.Sprintf("%p", wsp),
		}
	}

	wsp.Log().Debugf("begin listening on %s %s", wsp.Network(), laddr)

	// index listeners by local address
	// should live infinitely
	key := ListenerKey(fmt.Sprintf("%s:0.0.0.0:%d", wsp.network, laddr.Port))
	err = wsp.listeners.Put(key, listener)

	return err // should be nil here
}

func (wsp *wsProtocol) Send(target *Target, msg sip.Message) error {
	target = FillTargetHostAndPort(wsp.Network(), target)

	// validate remote address
	if target.Host == "" {
		return &ProtocolError{
			fmt.Errorf("empty remote target host"),
			fmt.Sprintf("send SIP message to %s %s", wsp.Network(), target.Addr()),
			fmt.Sprintf("%p", wsp),
		}
	}

	// resolve remote address
	raddr, err := wsp.resolveTarget(target)
	if err != nil {
		return err
	}

	// find or create connection
	conn, err := wsp.getOrCreateConnection(raddr)
	if err != nil {
		return err
	}

	logger := log.AddFieldsFrom(wsp.Log(), conn, msg)
	logger.Tracef("writing SIP message to %s %s", wsp.Network(), raddr)

	// send message
	_, err = conn.Write([]byte(msg.String()))

	return err
}

func (wsp *wsProtocol) resolveTarget(target *Target) (*net.TCPAddr, error) {
	addr := target.Addr()
	// resolve remote address
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, &ProtocolError{
			err,
			fmt.Sprintf("resolve target address %s %s", wsp.Network(), addr),
			fmt.Sprintf("%p", wsp),
		}
	}

	return raddr, nil
}

func (wsp *wsProtocol) getOrCreateConnection(raddr *net.TCPAddr) (Connection, error) {
	key := ConnectionKey(wsp.network + ":" + raddr.String())
	conn, err := wsp.connections.Get(key)
	if err != nil {
		wsp.Log().Debugf("connection for remote address %s %s not found, create a new one", wsp.Network(), raddr)

		tcpConn, err := net.DialTCP("tcp", nil, raddr)
		if err != nil {
			return nil, &ProtocolError{
				err,
				fmt.Sprintf("connect to %s %s address", wsp.Network(), raddr),
				fmt.Sprintf("%p", wsp),
			}
		}

		conn = NewConnection(tcpConn, key, wsp.Log())

		// todo upgrade to websockets?

		if err := wsp.connections.Put(conn, sockTTL); err != nil {
			return conn, err
		}
	}

	return conn, nil
}
