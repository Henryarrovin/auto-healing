package types

type Event struct {
	Kind      string
	Name      string
	Namespace string
	Reason    string
	Raw       string // serialised snippet forwarded to AI
}
