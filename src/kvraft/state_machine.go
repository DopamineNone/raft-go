package kvraft

type MemoryKVStateMachine struct {
	// field must be upper case for serialization
	Mp map[string]string
}

// Get the value associated with a key.
func (m *MemoryKVStateMachine) Get(key string) (string, Err) {
	value, ok := m.Mp[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

// Put a key-value pair into the state machine.
func (m *MemoryKVStateMachine) Put(key string, value string) Err {
	m.Mp[key] = value
	return OK
}

// Append a value to the current value associated with a key.
func (m *MemoryKVStateMachine) Append(key string, value string) Err {
	m.Mp[key] += value
	return OK
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		Mp: make(map[string]string),
	}
}
