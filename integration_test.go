package post

import (
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
	cm.Register(TextFormat, oid.Int2, &TextDecoder{})
	cm.Register(TextFormat, oid.Int4, &TextDecoder{})
	cm.Register(TextFormat, oid.Int8, &TextDecoder{})
	conn := NewConn(c, cm)
	err = conn.Connect(map[string]string{"user": "maciek"}, &DefaultAuthenticator{"maciek", ""})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func ensureValidAndClose(t *testing.T, conn *Conn) {
	rows, err := conn.SimpleQuery("select 1")
	defer func() {
		if err = conn.Close(); err != nil {
			t.Fatalf("want nil err; got %v", err)
		}
	}()
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	if !rows.Next() {
		t.Error("want one row; got none")
	}
	err = rows.Close()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestConnect(t *testing.T) {
	conn := connect(t)
	ensureValidAndClose(t, conn)
}

func TestQueryZeroRows(t *testing.T) {
	conn := connect(t)
	defer ensureValidAndClose(t, conn)
	rows, err := conn.SimpleQuery("SELECT 1 WHERE false")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	if rows.Next() {
		t.Fatal("want zero rows; got at least one")
	}
	err = rows.Close()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestQueryFields(t *testing.T) {
	conn := connect(t)
	defer ensureValidAndClose(t, conn)
	rows, err := conn.SimpleQuery("SELECT 'hello world' AS greeting")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	fields := rows.Fields()
	if fieldCount := len(fields); fieldCount != 1 {
		t.Fatalf("want 1 field; got %v", fieldCount)
	}
	field := fields[0]
	if field.Name != "greeting" {
		t.Errorf("want field 1 name 'greeting'; got %v", field.Name)
	}
	if field.TypeOid != oid.Unknown {
		t.Errorf("want field 1 type oid.Unknown; got %v", field.TypeOid)
	}
	err = rows.Close()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestQueryScanOneRow(t *testing.T) {
	conn := connect(t)
	defer ensureValidAndClose(t, conn)
	rows, err := conn.SimpleQuery("SELECT 'hello world' AS greeting")
	if err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	if !rows.Next() {
		t.Errorf("want nil rows.Err(); got %v", rows.Err())
		t.Fatal("want one row; got none")
	}
	var result string
	rows.Scan(&result)
	if result != "hello world" {
		t.Errorf("want result 'hello world'; got %v", result)
	}
	if rows.Next() {
		t.Fatal("want one row; got at least two")
	}
	err = rows.Close()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestQueryTwoRows(t *testing.T) {

}

func TestQueryTwoResultSets(t *testing.T) {

}

func TestQueryCloseEarly(t *testing.T) {
	// ensure that protocol state is re-established correctly if the result
	// row object is closed early
}

func TestQueryTwoResultsCloseEarly(t *testing.T) {
	// ensure that protocol state is re-established correctly if there are
	// multiple result objects and the row object is closed early
}
