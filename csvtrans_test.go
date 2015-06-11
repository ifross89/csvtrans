package csvtrans

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

func identTrans(_ int, in []string) ([]string, error) {
	return in, nil
}

func double(_ int, in []string) ([]string, error) {
	for i, record := range in {
		val, err := strconv.Atoi(record)
		if err != nil {
			return nil, err
		}
		in[i] = fmt.Sprintf("%d", val*2)
	}
	return in, nil
}

var goodRunTests = []struct {
	in        string
	out       string
	transform RowTransformer
}{
	{
		"a,b\n1,2\n",
		"a,b\n1,2\n",
		identTrans,
	},
	{
		"1,2,3\n4,5,6\n",
		"2,4,6\n8,10,12\n",
		double,
	},
}

func TestGoodRun(t *testing.T) {
	for _, test := range goodRunTests {
		b := &bytes.Buffer{}
		err := Run(strings.NewReader(test.in), b, test.transform)
		if err != nil {
			t.Fatalf("TestGoodRun expected no error for %+v, got err=%v", test, err)
		}

		res := b.String()
		if res != test.out {
			t.Fatalf("TestGoodRun fail: expected %s, got %s", test.out, res)
		}
	}
}

func TestGoodRunFile(t *testing.T) {
	in := "./test/test_input.csv"
	out := "./test/test_output.csv"

	// Remove file if exists
	defer os.Remove(out)

	err := RunFile(in, out, identTrans)
	if err != nil {
		t.Fatalf("RunFile failed, got err: %v", err)
	}

	inFile, err := ioutil.ReadFile(in)
	if err != nil {
		t.Fatalf("Error reading file %s: err: %v", in, err)
	}
	outFile, err := ioutil.ReadFile(out)
	if err != nil {
		t.Fatalf("Error reading file %s: err: %v", out, err)
	}

	if !bytes.Equal(inFile, outFile) {
		t.Fatalf("Expected input and output to be equal, in=%s, out=%s", inFile, outFile)
	}
}
