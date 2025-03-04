package relnotes

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/template"
	"time"

	"github.com/blang/semver/v4"
	"sigs.k8s.io/yaml"
)

//go:embed relnotes.gomd
var relnotesData []byte

//go:embed relnotes.gomdx
var relnotesDataMDX []byte

const templateName = "relnotes.gomd"
const templateNameMDX = "relnotes.gomdx"

type NoteType string

const (
	NoteTypeFeature  NoteType = "feature"
	NoteTypeBugFix   NoteType = "bugfix"
	NoteTypeSecurity NoteType = "security"
	NoteTypeChange   NoteType = "change"
)

type Note struct {
	Type  NoteType `json:"type,omitempty"`
	Title string   `json:"title,omitempty"`
	Body  string   `json:"body,omitempty"`
	Image string   `json:"image,omitempty"`
	Docs  string   `json:"docs,omitempty"`
	HRef  string   `json:"href,omitempty"`
}

type Release struct {
	Version    string     `json:"version,omitempty"`
	DateString string     `json:"date,omitempty"`
	Notes      []Note     `json:"notes,omitempty"`
	Date       *time.Time `json:"-"`
}

type ChangeLog struct {
	Styles         ReleaseStyles `json:"-"`
	DocTitle       string        `json:"docTitle,omitempty"`
	DocDescription string        `json:"docDescription,omitempty"`
	Items          []Release     `json:"items,omitempty"`
}

type ReleaseStyles struct {
	Main string
	Date string
	Note NoteStyles
}

type NoteStyles struct {
	Main        string
	Description string // contains icon and description
	Icon        string
	Title       string
	TitleNoLink string
	Body        string
	Image       string
}

func MakeReleaseNotes(input string, mdx bool) error {
	cl, err := readChangeLog(input)
	if err != nil {
		return err
	}
	cl.Styles = ReleaseStyles{
		Main: "release",
		Date: "release__date",
		Note: NoteStyles{
			Main:        "note",
			Description: "note__description",
			Icon:        "note__typeIcon",
			Title:       "note__title",
			TitleNoLink: "note__title_no_link",
			Body:        "note__body",
			Image:       "note__image",
		},
	}
	wr := bufio.NewWriter(os.Stdout)
	if err := applyTemplate(cl, mdx, wr); err != nil {
		return err
	}
	return wr.Flush()
}

func MakeVariables(input string) error {
	cl, err := readChangeLog(input)
	if err != nil {
		return err
	}
	if len(cl.Items) == 0 {
		return fmt.Errorf("unable to parse release version from %q", input)
	}
	v := cl.Items[0].Version
	sv, err := semver.Parse(v)
	if err != nil {
		return err
	}
	fmt.Printf("version: \"%s\"\ndlVersion: \"v%s\"\n", sv, sv)
	return nil
}

func (t *Release) UnmarshalJSON(data []byte) error {
	type Alias Release
	err := json.Unmarshal(data, (*Alias)(t))
	if err == nil && t.DateString != "" {
		if ts, tsErr := time.Parse("2006-01-02", t.DateString); tsErr == nil {
			t.Date = &ts
		}
	}
	return err
}

func readChangeLog(input string) (*ChangeLog, error) {
	data, err := os.ReadFile(input)
	if err != nil {
		return nil, err
	}
	var changeLog ChangeLog
	err = yaml.Unmarshal(data, &changeLog)
	if err != nil {
		return nil, err
	}
	return &changeLog, nil
}

func applyTemplate(data any, mdx bool, wr io.Writer) (err error) {
	var tpl *template.Template
	tn := templateName
	if mdx {
		tn = templateNameMDX
		tpl, err = template.New(tn).Parse(string(relnotesDataMDX))
	} else {
		tpl, err = template.New(tn).Parse(string(relnotesData))
	}
	if err == nil {
		err = tpl.ExecuteTemplate(wr, tn, data)
	}
	return err
}
