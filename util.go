package post

import (
	"crypto/md5"
	"fmt"
)

func MD5ManglePassword(user, password, salt string) string {
	return fmt.Sprintf("md5%s%s", MD5string(MD5string(password+user))+salt)
}

func MD5string(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}
