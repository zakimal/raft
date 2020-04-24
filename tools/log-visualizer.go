package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)


type Entry struct {
	timestamp string
	id string
	msg string
}

type TestLog struct {
	name string
	status string
	entries []Entry
	ids map[string]bool
}

const tmpl = `
<!doctype html>
<html lang='en'>
  <head>
    <title>{{.Title}}</title>
  </head>
  <style>
  table {
    font-family: "Courier New";
    border-collapse: collapse;
  }
  table, th, td {
    padding: 8px;
    border: 1px solid #cccccc;
  }
  td.testcell {
    background-color: #ffffff;
  }
  td.Follower {
    background-color: #ffffff;
  }
  td.Candidate {
    background-color: #e2e2a3;
  }
  td.Leader {
    background-color: #e6fff5;
  }
  td.Dead {
    background-color: #dddddd;
  }
  h1 {
    text-align: center;
  }
  </style>
<body>
  <h1>{{.Title}}</h1>
  <p></p>
  <table>
    <tr>
      {{range .Headers}}
      <th>{{.}}</th>
      {{end}}
    </tr>
    {{range .HtmlItems}}
    <tr>
      {{.}}
    </tr>
    {{end}}
  </table>
</body>
</html>
`

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
	Dead
)


func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("shold not reach here")
	}
}

func emitTestResult(dirName string, tl TestLog) {
	fileName := path.Join(dirName, tl.name + ".html")
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	t, err := template.New("page").Parse(tmpl)
	if err != nil {
		log.Fatal(err)
	}

	var numServers int
	var tested bool

	if _, ok := tl.ids["TEST"]; ok {
		tested = true
		numServers = len(tl.ids) - 1
	} else {
		tested = false
		numServers = len(tl.ids)
	}

	headers := []string{"Time"}
	if tested {
		headers = append(headers, "TEST")
	}
	for i := 0; i < numServers; i++ {
		headers = append(headers, strconv.Itoa(i))
	}

	serverState := make([]ServerState, numServers)

	var htmlItems []string
	for _, entry := range tl.entries {
		var b strings.Builder
		fmt.Fprintf(&b, "<td>%s</td>", entry.timestamp)
		if entry.id == "TEST" {
			if tested {
				fmt.Fprintf(&b, `  <td class="testcell">%s</td>`, entry.msg)
				for i := 0; i < numServers; i++ {
					fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
				}
			} else {
				log.Fatal("have TEST entry with no test IDs")
			}
		} else {
			idInt, err := strconv.Atoi(entry.id)
			if err != nil {
				log.Fatal(err)
			}

			if strings.Contains(entry.msg, "becomes Follower") {
				serverState[idInt] = Follower
			} else if strings.Contains(entry.msg, "listening") {
				serverState[idInt] = Follower
			} else if strings.Contains(entry.msg, "becomes Candidate") {
				serverState[idInt] = Candidate
			} else if strings.Contains(entry.msg, "becomes Leader") {
				serverState[idInt] = Leader
			} else if strings.Contains(entry.msg, "becomes Dead") {
				serverState[idInt] = Dead
			} else if strings.Contains(entry.msg, "created in state Follower") {
				serverState[idInt] = Follower
			}

			if tested {
				fmt.Fprintf(&b, "  <td class=\"testcell\"></td>")
			}

			for i := 0; i < idInt; i++ {
				fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
			}
			fmt.Fprintf(&b, `  <td class="%s">%s</td>`, serverState[idInt], entry.msg)
			for i := idInt + 1; i < numServers; i++ {
				fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
			}
		}
		htmlItems = append(htmlItems, b.String())
	}

	data := struct {
		Title string
		Headers []string
		HtmlItems []string
	}{
		Title: fmt.Sprintf("%s -- %s", tl.name, tl.status),
		Headers: headers,
		HtmlItems: htmlItems,
	}
	err = t.Execute(f, data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("... Emitted", "file://" + fileName)
}

func parseTestLogs(rd io.Reader) []TestLog {
	var testLogs []TestLog

	statusRE := regexp.MustCompile(`--- (\w+):\s+(\w+)`)
	entryRE := regexp.MustCompile(`([0-9:.]+) \[(\w+)\] (.*)`)

	scanner := bufio.NewScanner(bufio.NewReader(rd))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "=== RUN") {
			testLogs = append(testLogs, TestLog{ids: make(map[string]bool)})
			testLogs[len(testLogs)-1].name = strings.TrimSpace(line[7:])
		} else {
			if len(testLogs) == 0 {
				continue
			}
			curlog := &testLogs[len(testLogs)-1]

			statusMatch := statusRE.FindStringSubmatch(line)
			if len(statusMatch) > 0 {
				if statusMatch[2] != curlog.name {
					log.Fatalf("name on line %q mismatch with test name: got %s", line, curlog.name)
				}
				curlog.status = statusMatch[1]
				continue
			}

			entryMatch := entryRE.FindStringSubmatch(line)
			if len(entryMatch) > 0 {
				entry := Entry{
					timestamp: entryMatch[1],
					id:        entryMatch[2],
					msg:       entryMatch[3],
				}
				curlog.entries = append(curlog.entries, entry)
				curlog.ids[entry.id] = true
				continue
			}
		}
	}
	return testLogs
}

func main() {
	testLogs := parseTestLogs(os.Stdin)

	tnames := make(map[string]int)

	for i, tl := range testLogs {
		if count, ok := tnames[tl.name]; ok {
		testLogs[i].name = fmt.Sprintf("%s_%d", tl.name, count)
	}
		tnames[tl.name] += 1
	}

	statusSummary := "PASS"

	for _, tl := range testLogs {
		fmt.Println(tl.status, tl.name, tl.ids, "; entries:", len(tl.entries))
		if tl.status != "PASS" {
			statusSummary = tl.status
		}
		emitTestResult("/tmp", tl)
		fmt.Println("")
	}

	fmt.Println(statusSummary)
}
