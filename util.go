package post

import (
	"crypto/md5"
)

func MD5ManglePassword(user, password, salt string) string {
	fmt.Sprintf("md5%s%s", md5s(md5s(password+user))+salt)
}

func MD5string(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}
