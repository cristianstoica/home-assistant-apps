(function () {
  function parseTime(value) {
    var parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed / 1000 : null;
  }

  function cssVar(name) {
    return getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
  }

  // uPlot draws axis labels, ticks, and grid onto the canvas — CSS cannot reach
  // them. Colors are read from CSS at render time so charts adopt the palette
  // present at render (page load and HTMX fragment re-render).
  function themedAxis() {
    var axisColor = cssVar("--chart-axis");
    var gridColor = cssVar("--chart-grid");
    return {
      stroke: axisColor,
      grid: { stroke: gridColor },
      ticks: { stroke: gridColor }
    };
  }

  function emptyChart(el, text) {
    el.innerHTML = "";
    var node = document.createElement("p");
    node.className = "empty";
    node.textContent = text;
    el.appendChild(node);
  }

  function renderOverlay(el, payload) {
    if (!payload.valid_at || payload.valid_at.length === 0 || !window.uPlot) {
      emptyChart(el, "No overlay pairs.");
      return;
    }
    var forecastRaw = payload.forecast || [];
    var observedRaw = payload.observed || [];
    var xs = [];
    var forecast = [];
    var observed = [];
    payload.valid_at.forEach(function (value, index) {
      var t = parseTime(value);
      if (t === null) {
        return;
      }
      xs.push(t);
      forecast.push(forecastRaw[index] === undefined ? null : forecastRaw[index]);
      observed.push(observedRaw[index] === undefined ? null : observedRaw[index]);
    });
    if (xs.length === 0) {
      emptyChart(el, "No overlay pairs.");
      return;
    }
    el.innerHTML = "";
    new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: el.classList.contains("tall") ? 300 : 220,
      scales: { x: { time: true } },
      axes: [themedAxis(), themedAxis()],
      series: [
        {},
        { label: "Forecast", stroke: cssVar("--chart-1"), width: 2 },
        { label: "Observed", stroke: cssVar("--chart-2"), width: 2 }
      ]
    }, [xs, forecast, observed], el);
  }

  var SKILL_PALETTE = [
    "--chart-1",
    "--chart-2",
    "--chart-3",
    "--chart-4",
    "--chart-5",
    "--chart-6"
  ];

  function leadLabel(value) {
    if (value === 0) {
      return "Today";
    }
    if (value === 1) {
      return "Tomorrow";
    }
    return "+" + value + " days";
  }

  function renderSkill(el, payload) {
    var leads = payload.leads || [];
    var series = payload.series || [];
    // Explicit is-not-null test: 0.0 is a valid eligible point, so a truthiness
    // check would wrongly treat an all-zero-skill series as empty.
    var hasPoint = series.some(function (s) {
      return (s.skill || []).some(function (v) {
        return v !== null && v !== undefined;
      });
    });
    if (!window.uPlot || leads.length === 0 || !hasPoint) {
      emptyChart(el, "No skill curve yet.");
      return;
    }
    var uplotSeries = [{}];
    var data = [leads];
    series.forEach(function (s, index) {
      uplotSeries.push({
        label: s.label,
        stroke: cssVar(SKILL_PALETTE[index % SKILL_PALETTE.length]),
        width: 2,
        spanGaps: false
      });
      data.push(s.skill);
    });
    var xAxis = themedAxis();
    xAxis.values = function (self, splits) {
      return splits.map(leadLabel);
    };
    var yAxis = themedAxis();
    yAxis.label = "Skill";
    el.innerHTML = "";
    new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: 260,
      scales: { x: { time: false } },
      axes: [xAxis, yAxis],
      series: uplotSeries
    }, data, el);
  }

  function loadChart(el) {
    if (el.dataset.loaded === "true") {
      return;
    }
    el.dataset.loaded = "true";
    fetch(el.dataset.src, { credentials: "same-origin" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("chart fetch failed");
        }
        return response.json();
      })
      .then(function (payload) {
        if (el.dataset.chart === "overlay") {
          renderOverlay(el, payload);
        } else {
          renderSkill(el, payload);
        }
      })
      .catch(function () {
        emptyChart(el, "Chart data unavailable.");
      });
  }

  function bootCharts(root) {
    root.querySelectorAll("[data-chart][data-src]").forEach(loadChart);
  }

  document.addEventListener("DOMContentLoaded", function () {
    bootCharts(document);
  });
  document.body.addEventListener("htmx:afterSettle", function (event) {
    bootCharts(event.target);
  });
})();
