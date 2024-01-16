package main

/*
#include <stdio.h>

typedef union {
    int i;
    float f;
    char c;
} MyUnion;

int getUnionField(MyUnion* u) {
    return u->i;
}
*/
import "C"
import "fmt"

func main() {
    var u C.MyUnion
    u.i = 42

    // 通过指针传递给C函数获取union字段的值
    fieldValue := C.getUnionField((*C.MyUnion)(&u))

    fmt.Printf("Union field i: %d\n", int(fieldValue))
}

