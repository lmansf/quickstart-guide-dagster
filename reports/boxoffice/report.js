// Shared hover/focus tooltip: any element with a data-tip attribute shows it.
(function () {
  var tt = document.createElement('div');
  tt.id = 'tt';
  document.body.appendChild(tt);

  function show(el, x, y) {
    tt.textContent = el.getAttribute('data-tip');
    tt.style.display = 'block';
    var pad = 14;
    var w = tt.offsetWidth, h = tt.offsetHeight;
    var left = Math.min(x + pad, window.innerWidth - w - 8);
    var top = y - h - pad;
    if (top < 8) top = y + pad;
    tt.style.left = left + 'px';
    tt.style.top = top + 'px';
  }
  function hide() { tt.style.display = 'none'; }

  document.addEventListener('mousemove', function (e) {
    var el = e.target.closest && e.target.closest('[data-tip]');
    if (el) show(el, e.clientX, e.clientY); else hide();
  });
  document.addEventListener('focusin', function (e) {
    var el = e.target.closest && e.target.closest('[data-tip]');
    if (!el) return hide();
    var r = el.getBoundingClientRect();
    show(el, r.left + r.width / 2, r.top);
  });
  document.addEventListener('focusout', hide);
  window.addEventListener('scroll', hide, true);
})();
