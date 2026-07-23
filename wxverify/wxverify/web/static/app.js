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

  // Blended hourly drill-down: temp line (left axis), precip bars (right
  // axis), wind line (legend-only scale). Per-feed series are created hidden
  // and toggled via the "Show individual feeds" checkbox.
  function renderForecastHourly(el, payload) {
    var hours = payload.hours || [];
    var xs = [];
    var keep = [];
    hours.forEach(function (value, index) {
      var t = parseTime(value);
      if (t !== null) {
        xs.push(t);
        keep.push(index);
      }
    });
    if (!window.uPlot || xs.length === 0) {
      emptyChart(el, "No hourly data yet.");
      return;
    }
    function pick(values) {
      return keep.map(function (index) {
        var v = (values || [])[index];
        return v === undefined ? null : v;
      });
    }
    var blend = payload.blend || {};
    var series = [{}];
    var data = [xs];
    series.push({
      label: "Temp °C",
      scale: "t",
      stroke: cssVar("--chart-1"),
      width: 2
    });
    data.push(pick(blend.temp_c));
    series.push({
      label: "Precip mm",
      scale: "p",
      stroke: cssVar("--chart-2"),
      fill: cssVar("--chart-2"),
      width: 1,
      paths: uPlot.paths.bars({ size: [0.6, 100] }),
      points: { show: false }
    });
    data.push(pick(blend.precip_mm));
    series.push({
      label: "Wind km/h",
      scale: "w",
      stroke: cssVar("--chart-3"),
      width: 2
    });
    data.push(pick(blend.wind_kmh));
    var feedSeriesIdx = [];
    (payload.feeds || []).forEach(function (feed, feedIndex) {
      var color = cssVar(SKILL_PALETTE[feedIndex % SKILL_PALETTE.length]);
      [
        ["temp_c", "t", "temp"],
        ["precip_mm", "p", "precip"],
        ["wind_kmh", "w", "wind"]
      ].forEach(function (spec) {
        series.push({
          label: feed.label + " " + spec[2],
          scale: spec[1],
          stroke: color,
          width: 1,
          show: false
        });
        data.push(pick(feed[spec[0]]));
        feedSeriesIdx.push(series.length - 1);
      });
    });
    var xAxis = themedAxis();
    var tAxis = themedAxis();
    tAxis.scale = "t";
    tAxis.label = "°C";
    var pAxis = themedAxis();
    pAxis.scale = "p";
    pAxis.side = 1;
    pAxis.label = "mm";
    el.innerHTML = "";
    var chart = new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: 300,
      scales: {
        x: { time: true },
        p: {
          range: function (u, min, max) {
            return [0, Math.max(max || 0, 1)];
          }
        }
      },
      axes: [xAxis, tAxis, pAxis],
      series: series
    }, data, el);
    el.uplotInstance = chart;
    el.feedSeriesIdx = feedSeriesIdx;
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
        } else if (el.dataset.chart === "forecast-hourly") {
          renderForecastHourly(el, payload);
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

  // "Show individual feeds" checkbox: flips visibility of the hidden per-feed
  // series registered on the chart element by renderForecastHourly.
  document.body.addEventListener("change", function (event) {
    var target = event.target;
    if (!target || !target.matches("input[data-feed-toggle]")) {
      return;
    }
    var chartEl = document.getElementById(target.getAttribute("data-feed-toggle"));
    if (!chartEl || !chartEl.uplotInstance || !chartEl.feedSeriesIdx) {
      return;
    }
    chartEl.feedSeriesIdx.forEach(function (index) {
      chartEl.uplotInstance.setSeries(index, { show: target.checked });
    });
  });

  // "Updated X ago" stays honest between polls: the tiles fragment answers
  // 204 (no swap) while data is unchanged, so the text is re-derived
  // client-side from the data-updated-at timestamp once a minute.
  function refreshRelativeTimes() {
    document.querySelectorAll("[data-updated-at]").forEach(function (el) {
      var t = Date.parse(el.getAttribute("data-updated-at"));
      if (!Number.isFinite(t)) {
        return;
      }
      var seconds = Math.max(0, (Date.now() - t) / 1000);
      var text;
      if (seconds < 60) {
        text = "just now";
      } else if (seconds < 3600) {
        text = Math.floor(seconds / 60) + " min ago";
      } else if (seconds < 86400) {
        text = Math.floor(seconds / 3600) + " h ago";
      } else {
        text = Math.floor(seconds / 86400) + " d ago";
      }
      el.textContent = "Updated " + text;
    });
  }
  setInterval(refreshRelativeTimes, 60000);

  // Database import: POSTs the chosen file as a raw octet-stream body. htmx
  // cannot send a raw file body (hx-post encodes params, and multipart would
  // need a server-side parser), so this is a plain fetch. The CSRF token is
  // read from the meta tag exactly as the htmx configRequest hook does; the
  // ingress-prefixed URL is server-rendered into data-import-url.
  document.body.addEventListener("click", function (event) {
    var target = event.target;
    if (!target || !target.matches("#import-run")) {
      return;
    }
    var fileInput = document.getElementById("import-file");
    var result = document.getElementById("import-result");
    function show(text) {
      result.hidden = false;
      result.textContent = text;
    }
    var file = fileInput && fileInput.files && fileInput.files[0];
    if (!file) {
      show("Choose a database file first.");
      return;
    }
    var confirmed = window.confirm(
      "Replaces the ENTIRE database. Data collected since your export will be lost. A backup is saved automatically to /data. Continue?"
    );
    if (!confirmed) {
      return;
    }
    var token = document.querySelector('meta[name="csrf-token"]').content;
    show("Importing...");
    fetch(target.dataset.importUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream"
      },
      body: file
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (response.ok) {
            show(
              "Imported. Backup saved as " + payload.backup +
              ". Scores are rebuilding."
            );
          } else {
            show(payload.error || "Import failed.");
          }
        });
      })
      .catch(function () {
        show("Import failed.");
      });
  });

  // Database export: prepare-then-stream. A plain GET download would hold the
  // request open (no headers) through VACUUM INTO and trip HA ingress's
  // response-start timeout, so this POSTs /begin (with CSRF), polls /status
  // until ready, then fetch()es /download and saves the body as a local Blob.
  // The fetch is deliberate: triggering the download by navigating a bare
  // <a> routes it through Firefox/Zen's legacy navigation-download channel,
  // which Cloudflare/ingress cancels at ~30 s on large files; the Fetch
  // channel is not subject to that cut. The status/download GETs are safe
  // methods and carry no CSRF; begin sends no body/Content-Type so the
  // mutation guard's allowlist is not exercised.
  document.body.addEventListener("click", function (event) {
    var target = event.target;
    if (!target || !target.matches("#export-run")) {
      return;
    }
    var beginUrl = target.dataset.beginUrl;
    var base = target.dataset.exportBase;
    var result = document.getElementById("export-result");
    function show(text) {
      result.hidden = false;
      result.textContent = text;
    }
    var token = document.querySelector('meta[name="csrf-token"]').content;
    show("Preparing export...");
    fetch(beginUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: { "X-CSRF-Token": token }
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("begin failed");
        }
        return response.json();
      })
      .then(function (payload) {
        pollStatus(payload.export_id, 0);
      })
      .catch(function () {
        show("Export failed to start.");
      });

    var MAX_POLLS = 240;
    function pollStatus(exportId, attempts) {
      if (attempts >= MAX_POLLS) {
        show("Export timed out.");
        return;
      }
      fetch(base + "/status/" + exportId, { credentials: "same-origin" })
        .then(function (response) {
          if (!response.ok) {
            throw new Error("status failed");
          }
          return response.json();
        })
        .then(function (payload) {
          if (payload.state === "ready") {
            triggerDownload(base + "/download/" + exportId);
            show("Download started.");
          } else if (payload.state === "error") {
            show("Export failed.");
          } else {
            window.setTimeout(function () {
              pollStatus(exportId, attempts + 1);
            }, 750);
          }
        })
        .catch(function () {
          show("Export failed.");
        });
    }

    function formatBytes(bytes) {
      return (bytes / 1048576).toFixed(1) + " MB";
    }

    // The route sets Content-Disposition: attachment; the filename is
    // wxverify-<UTC timestamp>Z.db.gz (timestamp %Y%m%d-%H%M%S, not ISO-8601).
    // Falls back to a stable name if the header is absent or unparseable.
    function parseFilename(response) {
      var fallback = "wxverify-export.db.gz";
      var header = response.headers.get("Content-Disposition");
      if (!header) {
        return fallback;
      }
      var match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(header);
      if (!match || !match[1]) {
        return fallback;
      }
      try {
        return decodeURIComponent(match[1]);
      } catch (err) {
        return match[1];
      }
    }

    // Saves an in-memory Blob via a transient <a download>. The object URL is
    // revoked on a delayed tick: revoking synchronously right after click can
    // cancel the save in some browsers.
    function saveBlob(blob, filename) {
      var url = URL.createObjectURL(blob);
      var anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      window.setTimeout(function () {
        URL.revokeObjectURL(url);
      }, 60000);
    }

    // Retained-file fallback (0.8.3 safety net): on any fetch/read failure,
    // render a clickable browser-native download link into the result area so
    // the user can still download + Retry against the retained export file.
    function showFallbackLink(message, downloadUrl, filename) {
      result.hidden = false;
      result.textContent = "";
      result.appendChild(document.createTextNode(message + " "));
      var link = document.createElement("a");
      link.href = downloadUrl;
      link.download = filename;
      link.rel = "noopener";
      link.textContent = "Download database";
      result.appendChild(link);
    }

    // Downloads on the normal Fetch channel (not the legacy navigation-download
    // channel that Cloudflare/ingress cuts at ~30 s), streaming the body so the
    // status line can show bytes received. Ingress strips Content-Length
    // (chunked), so total size is unknown → bytes only, no percentage.
    function triggerDownload(downloadUrl) {
      var filename = "wxverify-export.db.gz";
      show("Downloading...");
      fetch(downloadUrl, { credentials: "same-origin" })
        .then(function (response) {
          if (!response.ok) {
            throw new Error("download failed");
          }
          filename = parseFilename(response);
          if (!response.body || !response.body.getReader) {
            return response.blob().then(function (blob) {
              saveBlob(blob, filename);
              show("Download complete. Saved.");
            });
          }
          var reader = response.body.getReader();
          var chunks = [];
          var received = 0;
          function pump() {
            return reader.read().then(function (chunk) {
              if (chunk.done) {
                var blob = new Blob(chunks, { type: "application/gzip" });
                saveBlob(blob, filename);
                show("Download complete. Saved.");
                return;
              }
              chunks.push(chunk.value);
              received += chunk.value.length;
              show("Downloading... " + formatBytes(received));
              return pump();
            });
          }
          return pump();
        })
        .catch(function () {
          showFallbackLink(
            "Download failed. Use this link to save the retained file:",
            downloadUrl,
            filename
          );
        });
    }
  });
})();
