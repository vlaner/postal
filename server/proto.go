package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/textproto"
)

func WrongLinesNumber(i int) error {
	return fmt.Errorf("proto: wrong lines count: expected 3 but got %d", i)
}

var (
	PUBLISH   = []byte("PUB")
	SUBSCRIBE = []byte("SUB")
	MESSAGE   = []byte("MSG")
)

type Proto struct {
	Command string
	Topic   string
	Data    string
}

func (p Proto) Marshal() []byte {
	return []byte(fmt.Sprintf("%s\r\n%s\r\n%s", p.Command, p.Topic, p.Data))
}

type ProtoReader struct {
	reader *textproto.Reader
}

func NewProtoReader(r io.Reader) *ProtoReader {
	rd := bufio.NewReader(r)
	return &ProtoReader{reader: textproto.NewReader(rd)}
}

func (p *ProtoReader) Parse() (Proto, error) {
	lines := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		line, err := p.reader.ReadLineBytes()
		if err != nil {
			return Proto{}, err
		}

		lines[i] = line
	}

	command := bytes.TrimSpace(lines[0])
	if !bytes.Equal(command, PUBLISH) && !bytes.Equal(command, SUBSCRIBE) && !bytes.Equal(command, MESSAGE) {
		return Proto{}, fmt.Errorf("proto: wrong command: expected one of %q, %q or %q but got %q", PUBLISH, SUBSCRIBE, MESSAGE, command)
	}

	return Proto{
		Command: string(command),
		Topic:   string(bytes.TrimSpace(lines[1])),
		Data:    string(bytes.TrimSpace(lines[2])),
	}, nil
}

type ProtoWriter struct {
	w io.Writer
}

func NewProtoWriter(w io.Writer) *ProtoWriter {
	return &ProtoWriter{w: w}
}

func (w *ProtoWriter) Write(val Proto) error {
	data := val.Marshal()
	_, err := w.w.Write(data)
	return err
}
