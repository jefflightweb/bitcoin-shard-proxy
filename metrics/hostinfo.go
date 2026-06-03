package metrics

import (
	"log/slog"
	"strconv"

	"github.com/lightwebinc/shard-common/hostinfo"
	promclient "github.com/prometheus/client_golang/prometheus"
)

// SetLevelVar registers the runtime log-level variable so [Recorder.Serve]
// exposes a /loglevel endpoint for runtime level inspection and change.
func (r *Recorder) SetLevelVar(lvl *slog.LevelVar) { r.levelVar = lvl }

// SetHostInfo publishes a slim bsp_host_info gauge (value 1) carrying
// low-cardinality host facts as labels, for dashboard joins with the
// host.inventory log event emitted at startup. Best-effort: registration
// errors are ignored so a duplicate or malformed label set never blocks start.
func (r *Recorder) SetHostInfo(inv hostinfo.Inventory) {
	var nic, speed string
	for _, ifc := range inv.Interfaces {
		if ifc.OperState == "up" && (len(ifc.IPv4) > 0 || len(ifc.IPv6) > 0) {
			nic = ifc.Name
			if ifc.SpeedMbps > 0 {
				speed = strconv.Itoa(ifc.SpeedMbps)
			}
			break
		}
	}
	g := promclient.NewGaugeVec(promclient.GaugeOpts{
		Name: "bsp_host_info",
		Help: "Static host facts (value always 1); join with host.inventory log on service_instance_id.",
	}, []string{
		"hostname", "kernel_version", "cpu_logical", "mem_bytes",
		"rmem_max", "nic", "speed_mbps", "version",
	})
	if err := r.promOtelReg.Register(g); err != nil {
		return
	}
	g.WithLabelValues(
		inv.Hostname,
		inv.KernelVersion,
		strconv.Itoa(inv.CPULogical),
		strconv.FormatUint(inv.MemTotalBytes, 10),
		inv.Sysctl["net.core.rmem_max"],
		nic,
		speed,
		inv.Version,
	).Set(1)
}
