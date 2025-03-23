package assignment

import "fmt"

func NewStrategy(s string) (Strategy, error) {
	switch s {
	case "sticky":
		return &StickyAssignor{}, nil
	case "round-robin":
		return &RoundRobinAssignor{}, nil
	default:
		return nil, fmt.Errorf("unknown assignment strategy: %s", s)
	}
}
