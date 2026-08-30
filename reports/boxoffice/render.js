// Renders the box office report from window.BOXOFFICE_DATA (see data.js, written
// by the boxoffice_dashboard_data Dagster asset). Two mechanisms:
//   <span data-fill="totals.net" data-fmt="usd0">   — inline numbers in prose
//   <div data-chart="revenue-by-event">             — chart containers, built here
//   <table data-table="events">                     — table twins, built here
// Values baked into the HTML act as a static fallback if the data file is absent.
(function () {
  var D = window.BOXOFFICE_DATA;
  if (!D) return;

  // ---- derived values the pages quote but the payload doesn't carry --------
  var best = D.derived.best_event, worst = D.derived.worst_event;
  D.computed = {
    buffer_pct: D.levers_total / D.goal.gap - 1,
    spread_vs_gap: (best.net - worst.net) / D.goal.gap,
    page2_levers_sum: D.levers[0].amount + D.levers[1].amount,
    page3_levers_sum: D.levers[2].amount + D.levers[3].amount,
    organic_share: D.attribution.organic_net / D.totals.net,
    worst_event_pct_of_house: D.derived.worst_event.sell_through,
    realloc_moved: D.levers[2].detail.budget_moved,
    refund_convert_pct: D.levers[3].detail.convert_rate,
    vip_lift_pct: D.levers[1].detail.price_lift,
    soft_target_pct: D.levers[0].detail.target_sell_through,
  };

  // ---- formatters ----------------------------------------------------------
  function commas(v) { return Math.round(v).toLocaleString('en-US'); }
  var fmt = {
    usd0: function (v) { return '$' + commas(v); },
    usdk: function (v) { return Math.abs(v) >= 10000 ? '$' + (v / 1000).toFixed(1) + 'K' : '$' + commas(v); },
    usd2: function (v) { return '$' + Number(v).toFixed(2); },
    plus_usd0: function (v) { return '+$' + commas(v); },
    int: commas,
    pct0: function (v) { return Math.round(v * 100) + '%'; },
    pct1: function (v) { return (v * 100).toFixed(1) + '%'; },
    x1: function (v) { return v.toFixed(1) + '×'; },
    pts0: function (v) { return Math.round(v) + ' pts'; },
    text: function (v) { return String(v); },
    dt: function (v) {
      var d = new Date(v);
      return isNaN(d) ? String(v) : d.toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' });
    },
  };
  function dateShort(iso) {
    var d = new Date(iso + 'T12:00:00Z');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  }
  function dowShort(iso) {
    var d = new Date(iso + 'T12:00:00Z');
    return d.toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' });
  }
  function get(obj, path) {
    return path.split('.').reduce(function (o, k) { return o == null ? o : o[k]; }, obj);
  }
  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // ---- inline fills --------------------------------------------------------
  document.querySelectorAll('[data-fill]').forEach(function (el) {
    var v = get(D, el.getAttribute('data-fill'));
    if (v == null) return;
    var f = fmt[el.getAttribute('data-fmt') || 'text'];
    if (f) el.textContent = f(v);
  });

  // ---- shared DOM builders -------------------------------------------------
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }
  function bar(widthPct, tip) {
    var b = el('div', 'bar');
    b.style.width = widthPct.toFixed(1) + '%';
    b.setAttribute('tabindex', '0');
    b.setAttribute('data-tip', tip);
    return b;
  }
  function flag(kind, text) {
    return el('span', 'flag ' + kind, text);
  }
  function barRow(container, label, sublabel, barEl, valText, flagEl) {
    var lab = el('div', 'rlabel', label);
    if (sublabel) lab.appendChild(el('small', null, sublabel));
    var row = el('div', 'rbar');
    row.appendChild(barEl);
    row.appendChild(el('span', 'val', valText));
    if (flagEl) row.appendChild(flagEl);
    container.appendChild(lab);
    container.appendChild(row);
  }

  // ---- chart renderers -----------------------------------------------------
  var charts = {};

  charts['bridge'] = function (root) {
    var max = Math.max.apply(null, D.levers.map(function (l) { return l.amount; }));
    var subs = {
      soft_night: worst.name + ' sold ' + fmt.pct0(worst.sell_through) + ' of the house',
      vip: 'VIP sold out ' + D.levers[1].detail.sellout_nights + ' of ' + D.levers[1].detail.nights + ' nights',
      realloc: 'same ' + fmt.usd0(D.totals.marketing_spend) + ' budget, better mix',
      refunds: fmt.usdk(D.totals.refunded_rev) + ' walked out the door',
    };
    var tips = {
      soft_night: 'Lift the weakest night from ' + fmt.pct0(worst.sell_through) + ' to ' +
        fmt.pct0(D.computed.soft_target_pct) + ' sell-through: +' + D.levers[0].detail.add_tickets +
        ' tickets × ~' + fmt.usd0(D.levers[0].detail.avg_net_per_ticket) + ' avg net. See page 2.',
      vip: 'VIP hits its cap ' + D.levers[1].detail.sellout_nights + ' nights of ' + D.levers[1].detail.nights +
        ' — demand outruns supply. +' + fmt.pct0(D.computed.vip_lift_pct) + ' VIP price on sellout nights (' +
        fmt.usdk(D.levers[1].detail.vip_net_on_sellouts) + ' of VIP net). See page 2.',
      realloc: 'Move ' + fmt.usd0(D.computed.realloc_moved) + ' from channels earning ' +
        fmt.usd2(D.levers[2].detail.source_rpd) + ' per dollar into channels earning ' +
        fmt.usd2(D.levers[2].detail.target_rpd) + ', discounted to ' +
        fmt.pct0(D.levers[2].detail.incrementality) + ' incrementality. See page 3.',
      refunds: 'Offer credit or swap-night instead of cash refunds; converting ' +
        fmt.pct0(D.computed.refund_convert_pct) + ' of the ' + fmt.usd0(D.totals.refunded_rev) +
        ' refunded. See page 3.',
    };
    D.levers.forEach(function (l) {
      barRow(root, l.label, subs[l.key], bar(78 * l.amount / max, tips[l.key]), fmt.plus_usd0(l.amount));
    });
  };

  charts['revenue-by-event'] = function (root) {
    var evs = D.events.slice().sort(function (a, b) { return b.net - a.net; });
    var max = evs[0].net;
    var worstId = worst.name;
    evs.forEach(function (e) {
      var tip = e.name + ' — ' + fmt.usd0(e.net) + ' net · ' + commas(e.sold) + ' tickets (' +
        (e.sell_through >= 1 ? 'sold out' : fmt.pct0(e.sell_through)) + ') · ' +
        fmt.usd0(e.refunded_rev) + ' refunded';
      var f = null;
      if (e.sell_through >= 1) f = flag('good', '● sold out');
      else if (e.name === worstId) f = flag('serious', '▲ the soft night');
      barRow(root, e.name, e.genre + ' · ' + dowShort(e.date) + ' ' + dateShort(e.date),
        bar(76 * e.net / max, tip), fmt.usd0(e.net), f);
    });
  };

  charts['sellthrough'] = function (root) {
    D.events.forEach(function (e) {
      var row = el('div', 'meter-row');
      row.appendChild(el('div', 'rlabel', dateShort(e.date) + ' · ' + e.name));
      var m = el('div', 'meter');
      m.setAttribute('tabindex', '0');
      var tip = commas(e.sold) + ' of ' + commas(e.capacity) + ' seats sold (' + fmt.pct0(e.sell_through) + ')';
      if (e.sell_through >= 1) tip = commas(e.sold) + ' of ' + commas(e.capacity) + ' — sold out';
      if (e.name === worst.name) tip += ' — ' + commas(worst.unsold_seats) + ' seats unsold, the season’s largest untapped inventory';
      m.setAttribute('data-tip', tip);
      var fill = el('i');
      if (e.sell_through >= 1) fill.className = 'full';
      fill.style.width = Math.min(100, e.sell_through * 100).toFixed(1) + '%';
      m.appendChild(fill);
      row.appendChild(m);
      row.appendChild(el('div', 'val', commas(e.sold) + ' · ' + fmt.pct0(e.sell_through) +
        (e.name === worst.name ? ' ▲' : '')));
      root.appendChild(row);
    });
  };

  charts['tiers'] = function (root) {
    var evs = D.events.slice().sort(function (a, b) { return b.net - a.net; });
    var max = evs[0].net;
    var tierCls = { GA: 't-ga', Balcony: 't-bal', VIP: 't-vip' };
    evs.forEach(function (e) {
      root.appendChild(el('div', 'rlabel', e.name));
      var row = el('div', 'rbar stack');
      ['GA', 'Balcony', 'VIP'].forEach(function (tier, i) {
        var t = e.tiers[tier];
        var seg = el('div', 'seg ' + tierCls[tier] + (i === 2 ? ' last' : ''));
        seg.style.width = (75 * t.net / max).toFixed(1) + '%';
        seg.setAttribute('tabindex', '0');
        seg.setAttribute('data-tip', e.name + ' ' + tier + ' — ' + fmt.usd0(t.net) + ' net · ' +
          t.sold + '/' + t.cap + ' sold' + (t.sold >= t.cap ? ' (cap)' : ''));
        row.appendChild(seg);
      });
      row.appendChild(el('span', 'val', fmt.usdk(e.net)));
      root.appendChild(row);
    });
  };

  charts['daily'] = function (root) {
    var NS = 'http://www.w3.org/2000/svg';
    var x0 = 56, x1 = 704, y0 = 218, y1 = 16, n = D.daily.length;
    var maxNet = Math.max.apply(null, D.daily.map(function (d) { return d.net; }));
    var step = 10000;
    var ymax = Math.max(step, Math.ceil(maxNet / step) * step);
    function X(i) { return x0 + i * (x1 - x0) / (n - 1); }
    function Y(v) { return y0 - (v / ymax) * (y0 - y1); }
    var svg = document.createElementNS(NS, 'svg');
    svg.setAttribute('viewBox', '0 0 720 260');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', root.getAttribute('data-aria') ||
      'Line chart of daily net revenue across the on-sale window.');
    function sel(tag, attrs, cls, text) {
      var node = document.createElementNS(NS, tag);
      Object.keys(attrs).forEach(function (k) { node.setAttribute(k, attrs[k]); });
      if (cls) node.setAttribute('class', cls);
      if (text != null) node.textContent = text;
      svg.appendChild(node);
      return node;
    }
    for (var g = step; g <= ymax; g += step) {
      sel('line', { x1: x0, y1: Y(g), x2: x1, y2: Y(g) }, 'svg-grid');
    }
    sel('line', { x1: x0, y1: y0, x2: x1, y2: y0 }, 'svg-axis');
    sel('text', { x: x0 - 8, y: y0 + 3, 'text-anchor': 'end' }, 'svg-tick', '$0');
    for (g = step; g <= ymax; g += step) {
      sel('text', { x: x0 - 8, y: Y(g) + 3, 'text-anchor': 'end' }, 'svg-tick', '$' + (g / 1000) + 'K');
    }
    for (var i = 0; i < n; i += 4) {
      sel('text', { x: X(i), y: 236, 'text-anchor': 'middle' }, 'svg-tick', dateShort(D.daily[i].date));
    }
    var pts = D.daily.map(function (d, j) { return X(j).toFixed(1) + ',' + Y(d.net).toFixed(1); });
    sel('path', { d: 'M' + x0 + ',' + y0 + ' L' + pts.join(' L') + ' L' + x1 + ',' + y0 + ' Z' }, 'svg-area');
    sel('polyline', { points: pts.join(' ') }, 'svg-line');
    sel('circle', { cx: X(0), cy: Y(D.daily[0].net), r: 4.5 }, 'svg-dot');
    sel('circle', { cx: X(n - 1), cy: Y(D.daily[n - 1].net), r: 4.5 }, 'svg-dot');
    sel('text', { x: X(0) + 10, y: Y(D.daily[0].net) + 5 }, 'svg-lab',
      fmt.usdk(D.daily[0].net) + ' on-sale day');
    sel('text', { x: X(n - 1) - 8, y: Y(D.daily[n - 1].net) - 8, 'text-anchor': 'end' }, 'svg-lab',
      fmt.usdk(D.daily[n - 1].net));
    D.daily.forEach(function (d, j) {
      var hit = sel('circle', { cx: X(j), cy: Y(d.net), r: 12, tabindex: 0 }, 'svg-hit');
      hit.setAttribute('data-tip', dowShort(d.date) + ' ' + dateShort(d.date) +
        (j === 0 ? ' (on-sale day)' : '') + ' — ' + fmt.usd0(d.net) + ' net · ' +
        commas(d.tickets) + ' tickets');
    });
    root.appendChild(svg);
  };

  charts['rpd'] = function (root) {
    var tracked = D.campaigns.filter(function (c) { return c.has_code && c.spend > 0; });
    var untracked = D.campaigns.filter(function (c) { return !c.has_code; });
    var max = Math.max.apply(null, tracked.map(function (c) { return c.rpd; }));
    var worstTracked = D.derived.worst_tracked_campaign.id;
    tracked.concat(untracked).forEach(function (c, idx) {
      var tip;
      if (!c.has_code) {
        tip = c.name + ' — ' + fmt.usd0(c.spend) + ' spent, zero orders attributed: the campaign ran ' +
          'with no promo code, so code-based attribution cannot see it at all';
      } else {
        tip = c.name + ' — ' + fmt.usd2(c.rpd) + ' net per $1 · ' + fmt.usd0(c.net) +
          ' attributed · ' + commas(c.tickets) + ' tickets';
      }
      var f = null;
      if (idx === 0) f = flag('good', '● best return');
      else if (c.id === worstTracked) f = flag('serious', '▲ worst tracked return');
      else if (!c.has_code) f = flag('critical', '✕ untracked — no promo code');
      barRow(root, c.name, c.channel.replace('_', ' ') + ' · ' + fmt.usd0(c.spend) + ' spend',
        bar(Math.max(0.3, 72 * c.rpd / max), tip), fmt.usd2(c.rpd), f);
    });
  };

  charts['dumbbell'] = function (root) {
    if (D.attribution.unattributed_dirty_net < 1) {
      root.appendChild(el('p', 'note',
        'Promo codes in the current data are already clean — nothing is unattributed, and ' +
        'recorded and cleaned attribution agree. This section shows movement only when dirty codes exist.'));
      return;
    }
    var rows = D.campaigns
      .filter(function (c) { return Math.abs(c.net - c.net_dirty) >= 300; })
      .sort(function (a, b) {
        return Math.abs(b.net - b.net_dirty) - Math.abs(a.net - a.net_dirty);
      })
      .slice(0, 4)
      .sort(function (a, b) { return b.net - a.net; })
      .map(function (c) {
        return { label: c.name, sub: null, pre: c.net_dirty, post: c.net,
          preTip: c.name + ' as recorded: ' + fmt.usd0(c.net_dirty),
          postTip: c.name + ' after cleanup: ' + fmt.usd0(c.net) + ' (+' + fmt.usd0(c.net - c.net_dirty) + ')' };
      });
    rows.push({
      label: '(unattributed)', sub: D.attribution.unattributed_dirty_orders + ' dirty-code orders',
      pre: D.attribution.unattributed_dirty_net, post: D.attribution.still_unattributed_net,
      preTip: 'Unattributed as recorded: ' + fmt.usd0(D.attribution.unattributed_dirty_net) +
        ' of net revenue matching no campaign',
      postTip: 'Unattributed after cleanup: ' + fmt.usd0(D.attribution.still_unattributed_net),
    });
    var max = Math.max.apply(null, rows.map(function (r) { return Math.max(r.pre, r.post); }));
    rows.forEach(function (r) {
      var drow = el('div', 'drow');
      var lab = el('div', 'rlabel', r.label);
      if (r.sub) lab.appendChild(el('small', null, r.sub));
      drow.appendChild(lab);
      var track = el('div', 'dtrack');
      var p1 = 92 * r.pre / max, p2 = 92 * r.post / max;
      var lo = Math.min(p1, p2), hi = Math.max(p1, p2);
      var join = el('span', 'join');
      join.style.left = lo + '%';
      join.style.width = Math.max(0.3, hi - lo) + '%';
      track.appendChild(join);
      [['pre', p1, r.preTip], ['post', p2, r.postTip]].forEach(function (spec) {
        var pt = el('span', 'pt ' + spec[0]);
        pt.style.left = spec[1] + '%';
        pt.setAttribute('tabindex', '0');
        pt.setAttribute('data-tip', spec[2]);
        track.appendChild(pt);
      });
      var valText = fmt.usdk(r.pre) + ' → ' + fmt.usdk(r.post);
      var val = el('span', 'dval', valText);
      if (hi > 60) { val.className = 'dval before'; val.style.left = (lo - 2) + '%'; }
      else { val.style.left = (hi + 2.5) + '%'; }
      track.appendChild(val);
      drow.appendChild(track);
      root.appendChild(drow);
    });
  };

  charts['showup'] = function (root) {
    var avg = D.derived.attendance.avg_rate;
    var bestName = D.derived.attendance.best.name, worstName = D.derived.attendance.worst.name;
    D.events.forEach(function (e) {
      if (!e.attendance) return;
      var a = e.attendance;
      var tip = e.name + ' — ' + fmt.pct1(a.show_up) + ' show-up · ' + commas(a.scanned) +
        ' of ' + commas(a.kept) + ' kept tickets scanned · ' + commas(a.no_shows) + ' no-shows';
      var f = null;
      if (e.name === bestName) f = flag('good', '● best');
      else if (e.name === worstName) {
        f = flag('serious', '▲ ' + Math.round((avg - a.show_up) * 100) + ' pts below avg');
      }
      barRow(root, dateShort(e.date) + ' · ' + e.name, null,
        bar(80 * a.show_up, tip), fmt.pct1(a.show_up), f);
    });
  };

  // ---- table twins ---------------------------------------------------------
  var tables = {};
  function fillTable(node, headers, rows) {
    var html = '<tr>' + headers.map(function (h) { return '<th>' + esc(h) + '</th>'; }).join('') + '</tr>';
    rows.forEach(function (r) {
      html += '<tr>' + r.map(function (c) { return '<td>' + c + '</td>'; }).join('') + '</tr>';
    });
    node.innerHTML = html;
  }
  tables['levers'] = function (node) {
    var basis = {
      soft_night: esc(worst.name) + ' ' + fmt.pct0(worst.sell_through) + ' → ' +
        fmt.pct0(D.computed.soft_target_pct) + ' sell-through; +' + D.levers[0].detail.add_tickets +
        ' tickets × ~' + fmt.usd0(D.levers[0].detail.avg_net_per_ticket) + ' net',
      vip: '+' + fmt.pct0(D.computed.vip_lift_pct) + ' VIP price on the ' +
        D.levers[1].detail.sellout_nights + ' sellout nights (' +
        fmt.usdk(D.levers[1].detail.vip_net_on_sellouts) + ' VIP net)',
      realloc: 'Shift ' + fmt.usd0(D.computed.realloc_moved) + ' from ' + fmt.usd2(D.levers[2].detail.source_rpd) +
        '/$ channels to ' + fmt.usd2(D.levers[2].detail.target_rpd) + '/$ channels; ' +
        fmt.pct0(D.levers[2].detail.incrementality) + ' incrementality',
      refunds: 'Convert ' + fmt.pct0(D.computed.refund_convert_pct) + ' of ' + fmt.usd0(D.totals.refunded_rev) +
        ' refund leakage to retained revenue',
    };
    var rows = D.levers.map(function (l) {
      return [esc(l.label), fmt.plus_usd0(l.amount), '<span style="display:block;text-align:left">' + basis[l.key] + '</span>'];
    });
    rows.push(['Total identified', fmt.plus_usd0(D.levers_total),
      '<span style="display:block;text-align:left">vs. ' + fmt.plus_usd0(D.goal.gap) + ' needed for +' +
      fmt.pct0(D.goal.pct) + ' YoY</span>']);
    fillTable(node, ['Lever', 'Estimated net', 'Basis'], rows);
  };
  tables['events'] = function (node) {
    var evs = D.events.slice().sort(function (a, b) { return b.net - a.net; });
    fillTable(node, ['Event', 'Date', 'Tickets', 'Sell-through', 'Gross', 'Refunded', 'Net'],
      evs.map(function (e) {
        return [esc(e.name), dateShort(e.date), commas(e.sold), fmt.pct0(e.sell_through),
          fmt.usd0(e.gross), fmt.usd0(e.refunded_rev), fmt.usd0(e.net)];
      }));
  };
  tables['tiers'] = function (node) {
    var evs = D.events.slice().sort(function (a, b) { return b.net - a.net; });
    fillTable(node, ['Event', 'GA net', 'GA sold', 'Balcony net', 'Balcony sold', 'VIP net', 'VIP sold'],
      evs.map(function (e) {
        function cell(t) { return [fmt.usd0(e.tiers[t].net), e.tiers[t].sold + '/' + e.tiers[t].cap]; }
        return [esc(e.name)].concat(cell('GA'), cell('Balcony'), cell('VIP'));
      }));
  };
  tables['daily'] = function (node) {
    var half = Math.ceil(D.daily.length / 2);
    var rows = [];
    for (var i = 0; i < half; i++) {
      var a = D.daily[i], b = D.daily[i + half];
      rows.push([dateShort(a.date), fmt.usd0(a.net), commas(a.tickets)]
        .concat(b ? [dateShort(b.date), fmt.usd0(b.net), commas(b.tickets)] : ['', '', '']));
    }
    fillTable(node, ['Date', 'Net revenue', 'Tickets', 'Date', 'Net revenue', 'Tickets'], rows);
  };
  tables['campaigns'] = function (node) {
    var rows = D.campaigns.map(function (c) {
      return [esc(c.name), esc(c.channel.replace('_', ' ')), fmt.usd0(c.spend), commas(c.orders),
        commas(c.tickets), fmt.usd0(c.net), c.spend > 0 ? fmt.usd2(c.rpd) : '—'];
    });
    rows.push(['(organic — no code)', '—', '$0', commas(D.attribution.organic_orders),
      commas(D.attribution.organic_tickets), fmt.usd0(D.attribution.organic_net), '—']);
    fillTable(node, ['Campaign', 'Channel', 'Spend', 'Orders', 'Tickets', 'Attributed net', 'Net per $1'], rows);
  };
  tables['attendance'] = function (node) {
    fillTable(node, ['Event', 'Date', 'Kept tickets', 'Scanned', 'Show-up', 'No-shows'],
      D.events.filter(function (e) { return e.attendance; }).map(function (e) {
        return [esc(e.name), dateShort(e.date), commas(e.attendance.kept), commas(e.attendance.scanned),
          fmt.pct1(e.attendance.show_up), commas(e.attendance.no_shows)];
      }));
  };

  document.querySelectorAll('[data-chart]').forEach(function (node) {
    var f = charts[node.getAttribute('data-chart')];
    if (f) { node.textContent = ''; f(node); }
  });
  document.querySelectorAll('[data-table]').forEach(function (node) {
    var f = tables[node.getAttribute('data-table')];
    if (f) f(node);
  });
})();
