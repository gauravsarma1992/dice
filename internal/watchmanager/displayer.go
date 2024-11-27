package watchmanager

type (
	Displayer interface {
		Display(string) error
		Close() error
	}

	ChannelSender struct {
		inpCh chan string
	}

	ArrayStorer struct {
		store []string
	}
)

func NewChannelSender() (displayer Displayer) {
	displayer = &ChannelSender{
		inpCh: make(chan string, 1000),
	}
	return
}

func (ch *ChannelSender) Display(input string) (err error) {
	ch.inpCh <- input
	return
}

func (ch *ChannelSender) Close() (err error) {
	return
}

func NewArrayStorer() (displayer Displayer) {
	displayer = &ArrayStorer{}
	return
}

func (arrSt *ArrayStorer) Display(input string) (err error) {
	arrSt.store = append(arrSt.store, input)
	return
}

func (arrSt *ArrayStorer) Close() (err error) {
	arrSt.store = make([]string, 0)
	return
}
