package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/fatih/structs"
)

type Unknown map[string]string

type CSVW struct {
	Context     string                `json:"@context"`
	URL         string                `json:"url"`
	Title       string                `json:"dct:title"`
	Description string                `json:"dct:description"`
	Issued      string                `json:"dct:issued"`
	Creator     Creator               `json:"dct:publisher"`
	Contact     models.ContactDetails `json:"dcat:contactPoint"`
	TableSchema Columns               `json:"tableSchema"`
	Theme       string                `json:"dcat:theme"`
	License     string                `json:"dct:license"`
	Frequency   string                `json:"dct:accrualPeriodicity"`
	Notes       []Note                `json:"notes"`
}

type Creator struct {
	Name string `json:"name"`
	Type string `json:"@type"`
	ID   string `json:"@id"` //a URL where more info is available
}

type Columns struct {
	C     []Column `json:"columns"`
	About string   `json:"aboutUrl"`
}

type Column map[string]interface{}

type Note struct {
	Type       string `json:"type"` // is this an enum?
	Target     string `json:"target"`
	Body       string `json:"body"`
	Motivation string `json:"motivation"` // how is this different from type? do we need this? is this an enum?
}

func main() {
	url := "https://api.beta.ons.gov.uk/v1/datasets/ashe-table-7-hours/editions/time-series/versions/1/metadata"

	metadata := getMetadata(url)

	csv := assignTopLevel(metadata)

	csv.TableSchema.About = url
	csv.TableSchema.C = populateColumns(metadata.Dimensions, metadata.UnitOfMeasure, metadata.Downloads.CSV.HRef)

	var alerts []models.Alert
	if metadata.Alerts != nil {
		alerts = *metadata.Alerts
	}

	var usage []models.UsageNote
	if metadata.UsageNotes != nil {
		usage = *metadata.UsageNotes
	}

	csv.Notes = addNotes(metadata.Downloads.CSV.HRef, alerts, usage)

	// for k, v := range values {
	// 	unk[dynamicKeys[k]] = v
	// }
	//
	// b := marshal(csv, unk)

	b, err := json.Marshal(csv)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(b))

}

func addNotes(url string, alerts []models.Alert, notes []models.UsageNote) []Note {
	var list []Note

	for _, a := range alerts {
		list = append(list, Note{
			Type:   a.Type,
			Body:   a.Description,
			Target: url,
		})
	}

	for _, u := range notes {
		list = append(list, Note{
			Type:   u.Title,
			Body:   u.Note,
			Target: url + "#col=need-to-store",
		})
	}

	return list
}

func populateColumns(dims []models.CodeList, unit, csvURL string) []Column {
	var list []Column

	headerRow := "V4_2,Data marking,Coefficient of variation,Time_codelist,Time,ashe-geography,Geography,Hours_codelist,Hours,Sex_codelist,Sex,WorkingPattern_codelist,WorkingPattern,Statistics_codelist,Statistics"
	header := strings.Split(headerRow, ",")

	parts := strings.Split(header[0], "_")
	if len(parts) != 2 {
		fmt.Println(parts[0] + " --- " + header[0])
		panic("not valid v4 header")
	}

	offset, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("not valid v4 header")
	}

	//observations
	col := Column{
		"titles":   header[0],
		"name":     unit,
		"datatype": "number",
		"required": true,
		"@id":      csvURL + "#col=0",
	}

	list = append(list, col)

	//data markings
	if offset != 0 {
		for i := 1; i <= offset; i++ {
			col := Column{
				"titles": header[i],
				"@id":    csvURL + "#col=" + strconv.Itoa(i),
			}
			list = append(list, col)
		}
	}

	offset += 1

	header = header[offset:]

	//dimensions
	for i := 0; i < len(header); i = i + 2 {

		codeHeader := header[i]
		dimHeader := header[i+1]
		dimHeader = strings.ToLower(dimHeader)

		var dim models.CodeList

		for _, d := range dims {
			if d.Name == dimHeader {
				dim = d
				break
			}
		}

		codeCol := Column{
			"name":     codeHeader,
			"@id":      csvURL + "#col=" + strconv.Itoa(offset+i),
			"valueURL": dim.HRef + "/codes/{" + codeHeader + "}", //how do we link to the code list or API?
			//"datatype": "number",
			"required": true,
		}

		labelCol := Column{
			"titles":      dim.Label,
			"name":        dimHeader,
			"description": dim.Description,
			"@id":         csvURL + "#col=" + strconv.Itoa(offset+i+1),
			//"datatype": "number",
			//"required": true,
		}

		list = append(list, codeCol, labelCol)
	}

	return list
}

func assignTopLevel(m *models.Metadata) *CSVW {
	return &CSVW{
		Context:     "http://www.w3.org/ns/csvw",
		URL:         m.Downloads.CSV.HRef,
		Title:       m.Title,
		Description: m.Description,
		Issued:      m.ReleaseDate,
		Theme:       m.Theme,
		License:     m.License,
		Frequency:   m.ReleaseFrequency,
		Contact:     m.Contacts[0],
		Creator: Creator{
			Name: m.Publisher.Name,
			Type: m.Publisher.Type,
			ID:   m.Publisher.HRef,
		},
	}
}

func getMetadata(url string) *models.Metadata {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("NewRequest: ", err)
		return nil
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Do: ", err)
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var md models.Metadata
	if err := json.Unmarshal(b, &md); err != nil {
		log.Println(err)
	}

	return &md
}

func marshal(st *CSVW, ourMap Unknown) []byte {
	structAsMap := structs.Map(st)

	for k, v := range ourMap {
		if _, ok := structAsMap[k]; ok {
			panic("key collision")
		}
		structAsMap[k] = v
	}

	bytes, err := json.Marshal(structAsMap)
	if err != nil {
		panic(err)
	}

	return bytes
}
