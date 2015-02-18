package post

import (
	"net"
	"testing"
)

func connect(t *testing.T) *Conn {
	c, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		t.Fatal(err)
	}
	conn := NewConn(c)
	err = conn.Connect(map[string]string{"user": "maciek"}, &DefaultAuthenticator{"maciek", ""})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestConnect(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
}

func TestQueryZeroRows(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	rows, err := conn.SimpleQuery("SELECT 1 WHERE false")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	// TODO: close the rows object with defer
	if rows.Next() {
		t.Fatal("want zero rows; got at least one")
	}
}

func TestQueryOneRow(t *testing.T) {

}

func TestQueryTwoRows(t *testing.T) {

}

func TestQueryTwoResultSets(t *testing.T) {

}
