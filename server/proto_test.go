package server_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/vlaner/postal/server"
)

func TestProto(t *testing.T) {
	msg := new(bytes.Buffer)
	msg.Write([]byte("PUB topic 4\r\ndata\r\n"))

	proto := server.NewProtoReader(msg)

	gotProto, err := proto.Parse()
	t.Logf("got error: %+v", err)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	t.Logf("%+v", gotProto)
}

func TestWrongLineNumber(t *testing.T) {
	testCases := []struct {
		desc          string
		amountOfLines int
	}{
		{
			desc:          "handle 0 lines",
			amountOfLines: 0,
		},
		{
			desc:          "handle 1 line",
			amountOfLines: 1,
		},
		{
			desc:          "handle 2 lines",
			amountOfLines: 2,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			msg := &bytes.Buffer{}
			msg.WriteString(strings.Repeat("PUB\r\n", tC.amountOfLines))
			proto := server.NewProtoReader(msg)

			_, err := proto.Parse()
			t.Logf("got error: %+v", err)
			if err == nil {
				t.Errorf("expected error but got nil")
			}
		})
	}
}

func TestWrongCommand(t *testing.T) {
	msg := &bytes.Buffer{}
	msg.Write([]byte("TEST topic 4\r\ndata"))

	proto := server.NewProtoReader(msg)
	_, err := proto.Parse()
	if err == nil {
		t.Errorf("expected error but got nil")
	}
}
func TestWrongTokenAmount(t *testing.T) {
	testCases := []struct {
		desc string
		msg  []byte
	}{
		{
			desc: "empty message",
			msg:  []byte("\r\n"),
		},
		{
			desc: "only publish command",
			msg:  []byte("PUB\r\n"),
		},
		{
			desc: "publish command and topic",
			msg:  []byte("PUB TEST\r\n"),
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			msg := &bytes.Buffer{}
			msg.Write(tC.msg)
			proto := server.NewProtoReader(msg)

			_, err := proto.Parse()
			t.Logf("got error: %+v", err)
			if err == nil {
				t.Errorf("expected error but got nil")
			}
		})
	}
}
