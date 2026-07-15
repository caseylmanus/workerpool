package main

func main() {
	engine := NewEngine()
	engine.Start()

	ui := NewUI(engine)
	ui.Run()
}
