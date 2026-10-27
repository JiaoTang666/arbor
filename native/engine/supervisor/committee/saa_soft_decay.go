package committee

import "math"

// saaSoftDecayConfig contains only continuous-policy parameters. Legacy SAA
// thresholds remain in RuntimeUnknownAdmissionManager for the frozen baseline.
type saaSoftDecayConfig struct {
	ObservationTau    float64
	AnchorTau         float64
	PersistenceTau    float64
	LoadTau           float64
	AnchorWeight      float64
	LocalityWeight    float64
	PersistenceWeight float64
	CrossWeight       float64
	LoadWeight        float64
	RiskWeight        float64
	Temperature       float64
	Margin            float64
}

func defaultSAASoftDecayConfig() saaSoftDecayConfig {
	return saaSoftDecayConfig{
		ObservationTau: 4, AnchorTau: 8, PersistenceTau: 12, LoadTau: 3,
		AnchorWeight: 0.30, LocalityWeight: 0.30, PersistenceWeight: 0.10,
		CrossWeight: 0.15, LoadWeight: 0.20, RiskWeight: 0.05,
		Temperature: 8, Margin: 0.05,
	}
}

func saaDecayFactor(delta int, tau float64) float64 {
	if delta <= 0 {
		return 0
	}
	if tau <= 0 {
		return 1
	}
	return 1 - math.Exp(-float64(delta)/tau)
}

func saaRetentionFactor(delta int, tau float64) float64 {
	if delta <= 0 {
		return 1
	}
	if tau <= 0 {
		return 0
	}
	return math.Exp(-float64(delta) / tau)
}

func saaEma(previous, observed float64, delta int, tau float64) float64 {
	alpha := saaDecayFactor(delta, tau)
	return saaClamp01(previous*(1-alpha) + saaClamp01(observed)*alpha)
}

func saaSigmoid(x float64) float64 {
	if x >= 40 {
		return 1
	}
	if x <= -40 {
		return 0
	}
	return 1 / (1 + math.Exp(-x))
}

func saaClamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func saaNormalize(value, maximum float64) float64 {
	if maximum <= 1e-9 {
		return 0
	}
	return saaClamp01(value / maximum)
}

func saaMigrationIntensity(scoreDelta float64, cfg saaSoftDecayConfig) float64 {
	temperature := cfg.Temperature
	if temperature <= 0 {
		temperature = 8
	}
	return saaSigmoid(temperature * (scoreDelta - cfg.Margin))
}
