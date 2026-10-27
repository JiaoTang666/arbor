package firstplacement

import (
	"strings"
	"testing"
	"time"
)

func fixture() Request {
	now := time.Unix(100, 0)
	return Request{ID: "q1", Sender: Endpoint{strings.Repeat("a", 40), true, false, 0}, Recipient: Endpoint{strings.Repeat("b", 40), false, true, 0}, MappingVersion: 3, AuthorityObservedAt: now, AuthorityReceivedAt: now, DecisionAt: now, Deadline: now.Add(time.Second), MaxAge: time.Second, BaselineSender: 0, BaselineRecipient: 1, Queues: []Queue{{10, now, now, 0}, {0, now, now, 0}}}
}
func TestPoliciesAndRelay(t *testing.T) {
	for p, want := range map[string]int{"baseline": 1, "follow_partner": 0, "shortest_queue": 1, "path_queue": 0} {
		d, e := Prepare(fixture(), p)
		if e != nil || d.Choice.Recipient != want {
			t.Fatalf("%s: %+v %v", p, d, e)
		}
	}
}
func TestRejectContracts(t *testing.T) {
	for name, change := range map[string]func(*Request){
		"existing":         func(r *Request) { r.Recipient.Exists = true },
		"unknown_absence":  func(r *Request) { r.Recipient.AbsenceConfirmed = false },
		"future_authority": func(r *Request) { r.AuthorityReceivedAt = r.DecisionAt.Add(time.Second) },
		"expired":          func(r *Request) { r.Deadline = r.DecisionAt.Add(-time.Second) },
		"scope":            func(r *Request) { r.HasHistoricalCandidates = true },
	} {
		t.Run(name, func(t *testing.T) {
			r := fixture()
			change(&r)
			if _, e := Prepare(r, "path_queue"); e == nil {
				t.Fatal("accepted")
			}
		})
	}
}
func TestStaleFallback(t *testing.T) {
	r := fixture()
	r.Queues[0].ObservedAt = r.DecisionAt.Add(-2 * time.Second)
	d, e := Prepare(r, "shortest_queue")
	if e != nil || d.Choice.Recipient != 0 || d.Reason == "selected" {
		t.Fatal(d, e)
	}
}
func TestReservationWatermarkAndFrozenInput(t *testing.T) {
	r := fixture()
	r.Queues[0].Operations = 0
	r.Reservations = []WorkReservation{{1, []float64{20, 0}}}
	d, e := Prepare(r, "shortest_queue")
	if e != nil || d.Choice.Recipient != 1 {
		t.Fatal(d, e)
	}
	r.Queues[0].IncludedReservation = 1
	r.Reservations[0].Work[0] = 99
	if d.Request.Reservations[0].Work[0] != 20 || d.Request.Queues[0].IncludedReservation != 0 {
		t.Fatal("aliased features")
	}
	d, e = Prepare(r, "shortest_queue")
	if e != nil || d.Choice.Recipient != 0 {
		t.Fatal("double count", e)
	}
}
func TestJointNewEndpoints(t *testing.T) {
	r := fixture()
	r.Sender.Exists = false
	r.Sender.AbsenceConfirmed = true
	d, e := Prepare(r, "path_queue")
	if e != nil || d.Choice.Sender != 1 || d.Choice.Recipient != 1 {
		t.Fatal(d, e)
	}
}
func TestActivationBeforeBind(t *testing.T) {
	d, _ := Prepare(fixture(), "path_queue")
	now := d.Request.DecisionAt
	c := Confirmation{"q1", 3, 4, 0, 0, now.Add(time.Millisecond)}
	if d.CanBind(c, now) == nil {
		t.Fatal("bound before activation")
	}
	if d.CanBind(c, now.Add(2*time.Second)) == nil {
		t.Fatal("late binding")
	}
	if e := d.CanBind(c, now.Add(2*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	c.Recipient = 1
	if d.CanBind(c, now.Add(2*time.Millisecond)) == nil {
		t.Fatal("wrong route")
	}
}
