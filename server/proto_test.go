package server_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/vlaner/postal/server"
)

func TestProto(t *testing.T) {
	msg := new(bytes.Buffer)
	msg.Write([]byte("PUB\r\ntopic\r\ndata"))

	proto := server.NewProtoReader(msg)

	_, err := proto.Parse()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
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
			if err == nil {
				t.Errorf("expected error but got nil")
			}
		})
	}
}
