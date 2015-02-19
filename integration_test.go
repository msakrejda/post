package post

import (
	"fmt"
	"net"
	"testing"
	"github.com/uhoh-itsmaciek/post/oid"
)

func connect(t *testing.T) *Conn {
	c, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		t.Fatal(err)
	}
	cm := NewCodecManager()
	cm.Register(TextFormat, oid.Text, &TextDecoder{})
	cm.Register(TextFormat, oid.Unknown, &TextDecoder{})
	conn := NewConn(c, cm)
	err = conn.Connect(map[string]string{"user": "maciek"}, &DefaultAuthenticator{"maciek", ""})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestConnect(t *testing.T) {
	conn := connect(t)
	err := conn.Close()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestQueryZeroRows(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	rows, err := conn.SimpleQuery("SELECT 1 WHERE false")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	// TODO: close the rows object
	if rows.Next() {
		t.Fatal("want zero rows; got at least one")
	}
}

func TestQueryFields(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	rows, err := conn.SimpleQuery("SELECT 'hello world' AS greeting")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	fields := rows.Fields()
	if fieldCount := len(fields); fieldCount != 1 {
		t.Fatalf("want 1 field; got %v", fieldCount)
	}
	// TODO: close the rows object
}

func TestQueryScanOneRow(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	rows, err := conn.SimpleQuery("SELECT 'hello world' AS greeting")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	// TODO: close the rows object
	if !rows.Next() {
		fmt.Println(rows.Err())
		t.Fatal("want one row; got none")
	}
	var result int
	rows.Scan(&result)
	if rows.Next() {
		t.Fatal("want one row; got at least two")
	}
}

func TestQueryTwoRows(t *testing.T) {

}

func TestQueryTwoResultSets(t *testing.T) {

}

func TestQueryTwice(t *testing.T) {
	// ensure that protocol state is re-established correctly after a query
}

func TestQueryCloseEarly(t *testing.T) {
	// ensure that protocol state is re-established correctly if the result
	// row object is closed early
}

