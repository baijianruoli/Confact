package util

import (
	"fmt"
	"testing"
)

//func TestBYteToString(t *testing.T) {
//	type args struct {
//		b byte
//	}
//	tests := []struct {
//		name string
//		args args
//		want string
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := BYteToString(tt.args.b); got != tt.want {
//				t.Errorf("BYteToString() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func BenchmarkStringToByte(b *testing.B) {
	s := ""
	for i := 0; i < 1e5; i++ {
		s += "123"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StringToByte(s)
	}

}

func TestStringToByte(t *testing.T) {
	s := StringToByte("123")
	g := StringToByte("456")
	fmt.Println(s)
	fmt.Println(g)
}
