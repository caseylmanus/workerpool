package main

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// UI manages the graphical interface for the Darwinian Concurrency Supervisor.
type UI struct {
	Engine *Engine
	App    fyne.App
	Window fyne.Window

	// DNA Sidebar Labels
	lblWorkers *widget.Label
	lblLatency *widget.Label
	lblBatch   *widget.Label
	lblDelay   *widget.Label
	lblKp      *widget.Label
	lblMode    *widget.Label

	// Live Telemetry Panel Labels
	lblTotalIngested    *widget.Label
	progressReliability *widget.ProgressBar
	lblSuccessCount     *widget.Label
	lblFailureCount     *widget.Label
}

// NewUI creates a new UI instance.
func NewUI(e *Engine) *UI {
	myApp := app.New()
	myWindow := myApp.NewWindow("Darwinian Concurrency Supervisor")

	ui := &UI{
		Engine: e,
		App:    myApp,
		Window: myWindow,
	}

	ui.setupWidgets()
	return ui
}

func (ui *UI) setupWidgets() {
	ui.lblWorkers = widget.NewLabel("")
	ui.lblLatency = widget.NewLabel("")
	ui.lblBatch = widget.NewLabel("")
	ui.lblDelay = widget.NewLabel("")
	ui.lblKp = widget.NewLabel("")
	ui.lblMode = widget.NewLabel("")

	dnaContainer := widget.NewCard("Current Strategy DNA", "Hot-swapped constants by GA Layer",
		container.NewVBox(
			container.NewHBox(widget.NewLabel("Active Target Pool:"), ui.lblWorkers),
			container.NewHBox(widget.NewLabel("PID Target Latency:"), ui.lblLatency),
			container.NewHBox(widget.NewLabel("Optimal Batch Size:"), ui.lblBatch),
			container.NewHBox(widget.NewLabel("Floor Backoff Delay:"), ui.lblDelay),
			container.NewHBox(widget.NewLabel("Proportional Kp Gain:"), ui.lblKp),
			container.NewHBox(widget.NewLabel("Supervisor Status: "), ui.lblMode),
		),
	)

	ui.lblTotalIngested = widget.NewLabel("0.00 MB")
	ui.progressReliability = widget.NewProgressBar()
	ui.lblSuccessCount = widget.NewLabel("0")
	ui.lblFailureCount = widget.NewLabel("0")

	telemetryContainer := widget.NewCard("Live Metrics Plane", "Atomic telemetry polling",
		container.NewVBox(
			container.NewHBox(widget.NewLabel("Total Text Corpus Extracted:"), ui.lblTotalIngested),
			widget.NewLabel("Pool Execution Reliability Score:"),
			ui.progressReliability,
			container.NewHBox(widget.NewLabel("Generation Page Ingestions:"), ui.lblSuccessCount),
			container.NewHBox(widget.NewLabel("Dropped Requests / Limits:  "), ui.lblFailureCount),
		),
	)

	mainGrid := container.NewGridWithColumns(2, dnaContainer, telemetryContainer)
	ui.Window.SetContent(container.NewVBox(
		widget.NewLabelWithStyle("DARWINIAN CONCURRENCY ENGINE OBSERVER", fyne.TextAlignCenter, fyne.TextStyle{Bold: true}),
		mainGrid,
	))

	ui.Window.Resize(fyne.NewSize(700, 320))
}

// Run starts the UI refresh loop and shows the window.
func (ui *UI) Run() {
	go ui.refreshLoop()
	ui.Window.ShowAndRun()
}

func (ui *UI) refreshLoop() {
	for {
		time.Sleep(200 * time.Millisecond)
		fyne.Do(func() {
			ui.refresh()
		})
	}
}

func (ui *UI) refresh() {
	// Refresh UI Strategy Variables
	ui.lblWorkers.SetText(fmt.Sprintf("%d Goroutines", atomic.LoadInt32(&ui.Engine.ActiveWorkers)))
	ui.lblLatency.SetText(fmt.Sprintf("%v", time.Duration(atomic.LoadInt64(&ui.Engine.PID.Setpoint))))
	ui.lblBatch.SetText(fmt.Sprintf("%d URLs", atomic.LoadInt32(&ui.Engine.CurrentBatch)))
	ui.lblDelay.SetText(fmt.Sprintf("%v", time.Duration(atomic.LoadInt64(&ui.Engine.CurrentMinDelay))))
	ui.lblKp.SetText(fmt.Sprintf("%.3f", math.Float64frombits(atomic.LoadUint64(&ui.Engine.PID.Kp))))
	ui.lblMode.SetText(ui.Engine.UIMode)

	// Process Cumulative MB from clean thread-safe bytes tally
	rawBytes := atomic.LoadUint64(&ui.Engine.Telemetry.CumulativeBytes)
	mb := float64(rawBytes) / (1024 * 1024)
	ui.lblTotalIngested.SetText(fmt.Sprintf("%.2f MB", mb))

	succ := atomic.LoadUint64(&ui.Engine.Telemetry.Successes)
	fail := atomic.LoadUint64(&ui.Engine.Telemetry.Failures)
	ui.lblSuccessCount.SetText(fmt.Sprintf("%d", succ))
	ui.lblFailureCount.SetText(fmt.Sprintf("%d", fail))

	total := succ + fail
	if total > 0 {
		ui.progressReliability.SetValue(float64(succ) / float64(total))
	} else {
		ui.progressReliability.SetValue(1.0)
	}
}
