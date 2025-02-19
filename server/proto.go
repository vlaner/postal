package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
)

func WrongTokensNumber(expected, got int) error {
	return fmt.Errorf("proto: wrong tokens count: expected %d but got %d", expected, got)
}

func WrongCommand(cmd string) error {
	return fmt.Errorf("proto: wrong command: expected one of %q, %q, %q  or %q but got %q", PUBLISH, SUBSCRIBE, MESSAGE, UNSUBSCRIBE, cmd)
}

var ErrInvalidProto = errors.New("invalid proto data")

var (
	PUBLISH     = []byte("PUB")
	SUBSCRIBE   = []byte("SUB")
	UNSUBSCRIBE = []byte("UNSUB")
	MESSAGE     = []byte("MSG")
	ACK         = []byte("ACK")
	SCHEMA      = []byte("SCHEMA")
)

type Proto struct {
	MessageID  string
	Command    string
	Topic      string
	PayloadLen int
	Data       []byte
	Schema     string
}

func (p Proto) Marshal() []byte {
	switch p.Command {
	case string(MESSAGE):
		return []byte(fmt.Sprintf("%s %s %s %d\r\n%s\r\n", p.Command, p.Topic, p.MessageID, len(p.Data), p.Data))
	}

	return nil
}

type ProtoReader struct {
	reader *textproto.Reader
}

func NewProtoReader(r io.Reader) *ProtoReader {
	rd := bufio.NewReader(r)
	return &ProtoReader{reader: textproto.NewReader(rd)}
}

func (p *ProtoReader) Parse() (Proto, error) {
	line, err := p.reader.ReadLineBytes()
	if err != nil {
		return Proto{}, err
	}

	line = bytes.TrimSpace(line)
	tokens := bytes.Split(line, []byte(" "))
	if len(tokens) < 1 {
		return Proto{}, WrongTokensNumber(1, 0)
	}

	switch {
	case bytes.HasPrefix(line, PUBLISH):
		if len(tokens) < 3 {
			return Proto{}, WrongTokensNumber(3, len(tokens))
		}

		payloadLen, err := strconv.Atoi(string(tokens[2]))
		if err != nil {
			return Proto{}, err
		}

		payload := make([]byte, payloadLen)
		_, err = p.reader.R.Read(payload)
		if err != nil {
			return Proto{}, err
		}

		return Proto{
			Command:    string(PUBLISH),
			Topic:      string(tokens[1]),
			PayloadLen: payloadLen,
			Data:       payload,
		}, nil

	case bytes.HasPrefix(line, SUBSCRIBE):
		if len(tokens) < 2 {
			return Proto{}, WrongTokensNumber(2, len(tokens))
		}

		return Proto{
			Command: string(SUBSCRIBE),
			Topic:   string(tokens[1]),
		}, nil

	case bytes.HasPrefix(line, UNSUBSCRIBE):
		if len(tokens) < 2 {
			return Proto{}, WrongTokensNumber(2, len(tokens))
		}

		return Proto{
			Command: string(UNSUBSCRIBE),
			Topic:   string(tokens[1]),
		}, nil

	case bytes.HasPrefix(line, ACK):
		if len(tokens) < 2 {
			return Proto{}, WrongTokensNumber(2, len(tokens))
		}

		return Proto{
			Command:   string(ACK),
			MessageID: string(tokens[1]),
		}, nil

	case bytes.HasPrefix(line, SCHEMA):
		if len(tokens) < 3 {
			return Proto{}, WrongTokensNumber(3, len(tokens))
		}

		schemaLen, err := strconv.Atoi(string(tokens[2]))
		if err != nil {
			return Proto{}, err
		}

		schemaBytes := make([]byte, schemaLen)
		_, err = p.reader.R.Read(schemaBytes)
		if err != nil {
			return Proto{}, err
		}

		return Proto{
			Command: string(SCHEMA),
			Topic:   string(tokens[1]),
			Schema:  string(schemaBytes),
		}, nil
	}

	return Proto{}, WrongCommand(string(tokens[0]))
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
