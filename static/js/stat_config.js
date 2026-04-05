// stat_config.js — Single authoritative source for stat directions and formatting.
// higherIsBetter: true = pink for high values (elite), blue for low
//                 false = pink for low values (elite), blue for high

const STAT_CONFIG = {
  // STANDARD BATTING — Higher = better
  games:    { label: 'G',     higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  pa:       { label: 'PA',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  ab:       { label: 'AB',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  r:        { label: 'R',     higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  h:        { label: 'H',     higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  doubles:  { label: '2B',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  triples:  { label: '3B',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  hr:       { label: 'HR',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  rbi:      { label: 'RBI',   higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  bb:       { label: 'BB',    higherIsBetter: true,  format: 'int',  group: 'standard_batting' },
  avg:      { label: 'AVG',   higherIsBetter: true,  format: 'avg',  group: 'standard_batting' },
  obp:      { label: 'OBP',   higherIsBetter: true,  format: 'avg',  group: 'standard_batting' },
  slg:      { label: 'SLG',   higherIsBetter: true,  format: 'avg',  group: 'standard_batting' },
  ops:      { label: 'OPS',   higherIsBetter: true,  format: 'ops',  group: 'standard_batting' },
  // Lower = better
  so:       { label: 'SO',    higherIsBetter: false, format: 'int',  group: 'standard_batting' },

  // ADVANCED BATTING — Higher = better
  ops_plus: { label: 'OPS+',  higherIsBetter: true,  format: 'int',  group: 'advanced_batting' },
  iso:      { label: 'ISO',   higherIsBetter: true,  format: 'avg',  group: 'advanced_batting' },
  babip:    { label: 'BABIP', higherIsBetter: true,  format: 'avg',  group: 'advanced_batting' },
  bb_pct:   { label: 'BB%',   higherIsBetter: true,  format: 'pct',  group: 'advanced_batting' },
  bb_per_k: { label: 'BB/K',  higherIsBetter: true,  format: 'dec2', group: 'advanced_batting' },
  rc:       { label: 'RC',    higherIsBetter: true,  format: 'dec1', group: 'advanced_batting' },
  // Lower = better
  k_pct:    { label: 'K%',    higherIsBetter: false, format: 'pct',  group: 'advanced_batting' },
  ab_per_hr:{ label: 'AB/HR', higherIsBetter: false, format: 'dec1', group: 'advanced_batting' },

  // STANDARD PITCHING — Higher = better
  g_p:      { label: 'G',     higherIsBetter: true,  format: 'int',  group: 'standard_pitching' },
  gs:       { label: 'GS',    higherIsBetter: true,  format: 'int',  group: 'standard_pitching' },
  w:        { label: 'W',     higherIsBetter: true,  format: 'int',  group: 'standard_pitching' },
  sv:       { label: 'SV',    higherIsBetter: true,  format: 'int',  group: 'standard_pitching' },
  ip:       { label: 'IP',    higherIsBetter: true,  format: 'dec1', group: 'standard_pitching' },
  k_p:      { label: 'K',     higherIsBetter: true,  format: 'int',  group: 'standard_pitching' },
  // Lower = better
  l:        { label: 'L',     higherIsBetter: false, format: 'int',  group: 'standard_pitching' },
  era:      { label: 'ERA',   higherIsBetter: false, format: 'era',  group: 'standard_pitching' },
  whip:     { label: 'WHIP',  higherIsBetter: false, format: 'era',  group: 'standard_pitching' },
  bb_p:     { label: 'BB',    higherIsBetter: false, format: 'int',  group: 'standard_pitching' },
  h_p:      { label: 'H',     higherIsBetter: false, format: 'int',  group: 'standard_pitching' },
  hr_p:     { label: 'HR',    higherIsBetter: false, format: 'int',  group: 'standard_pitching' },
  baa:      { label: 'BAA',   higherIsBetter: false, format: 'avg',  group: 'standard_pitching' },

  // ADVANCED PITCHING — Higher = better
  era_plus: { label: 'ERA+',  higherIsBetter: true,  format: 'int',  group: 'advanced_pitching' },
  k_per_6:  { label: 'K/6',   higherIsBetter: true,  format: 'dec2', group: 'advanced_pitching' },
  k_pct_p:  { label: 'K%',    higherIsBetter: true,  format: 'pct',  group: 'advanced_pitching' },
  lob_pct:  { label: 'LOB%',  higherIsBetter: true,  format: 'pct',  group: 'advanced_pitching' },
  // Lower = better
  bb_per_6: { label: 'BB/6',  higherIsBetter: false, format: 'dec2', group: 'advanced_pitching' },
  hr_per_6: { label: 'HR/6',  higherIsBetter: false, format: 'dec2', group: 'advanced_pitching' },
  bb_pct_p: { label: 'BB%',   higherIsBetter: false, format: 'pct',  group: 'advanced_pitching' },
  babip_p:  { label: 'BABIP', higherIsBetter: false, format: 'avg',  group: 'advanced_pitching' },

  // HQ BATTING — Higher = better
  pa_hq:      { label: 'PA',    higherIsBetter: true,  format: 'int',  group: 'hq_batting' },
  avg_hq:     { label: 'AVG',   higherIsBetter: true,  format: 'avg',  group: 'hq_batting' },
  obp_hq:     { label: 'OBP',   higherIsBetter: true,  format: 'avg',  group: 'hq_batting' },
  slg_hq:     { label: 'SLG',   higherIsBetter: true,  format: 'avg',  group: 'hq_batting' },
  ops_hq:     { label: 'OPS',   higherIsBetter: true,  format: 'ops',  group: 'hq_batting' },
  hr_hq:      { label: 'HR',    higherIsBetter: true,  format: 'int',  group: 'hq_batting' },
  bb_pct_hq:  { label: 'BB%',   higherIsBetter: true,  format: 'pct',  group: 'hq_batting' },
  iso_hq:     { label: 'ISO',   higherIsBetter: true,  format: 'avg',  group: 'hq_batting' },
  babip_hq:   { label: 'BABIP', higherIsBetter: true,  format: 'avg',  group: 'hq_batting' },
  k_pct_hq:   { label: 'K%',    higherIsBetter: false, format: 'pct',  group: 'hq_batting' },

  // HQ PITCHING — Lower = better (they're facing HQ batters)
  bf_hq:       { label: 'BF',    higherIsBetter: true,  format: 'int',  group: 'hq_pitching' },
  era_hq:      { label: 'ERA',   higherIsBetter: false, format: 'era',  group: 'hq_pitching' },
  obp_hq_p:    { label: 'OBP',   higherIsBetter: false, format: 'avg',  group: 'hq_pitching' },
  baa_hq:      { label: 'BAA',   higherIsBetter: false, format: 'avg',  group: 'hq_pitching' },
  k_pct_hq_p:  { label: 'K%',    higherIsBetter: true,  format: 'pct',  group: 'hq_pitching' },
  bb_pct_hq_p: { label: 'BB%',   higherIsBetter: false, format: 'pct',  group: 'hq_pitching' },
  k_per_6_hq:  { label: 'K/6',   higherIsBetter: true,  format: 'dec2', group: 'hq_pitching' },
  bb_per_6_hq: { label: 'BB/6',  higherIsBetter: false, format: 'dec2', group: 'hq_pitching' },
  hr_per_6_hq: { label: 'HR/6',  higherIsBetter: false, format: 'dec2', group: 'hq_pitching' },
};

/**
 * Returns a CSS rgb() color for a given percentile value.
 * Blue (#99c9ea) = low percentile, White = 50th, Pink (#d5539b) = high percentile.
 * For stats where lower is better (higherIsBetter=false), a low raw percentile
 * means the player has an ELITE value, so we invert before coloring.
 */
function getPercentileColor(percentile, higherIsBetter) {
  if (percentile === null || percentile === undefined) return 'rgb(255,255,255)';
  const colorPct = higherIsBetter ? percentile : (100 - percentile);
  const blue  = { r: 153, g: 201, b: 234 };
  const white = { r: 255, g: 255, b: 255 };
  const pink  = { r: 213, g:  83, b: 155 };
  let r, g, b;
  if (colorPct <= 50) {
    const t = colorPct / 50;
    r = Math.round(blue.r + t * (white.r - blue.r));
    g = Math.round(blue.g + t * (white.g - blue.g));
    b = Math.round(blue.b + t * (white.b - blue.b));
  } else {
    const t = (colorPct - 50) / 50;
    r = Math.round(white.r + t * (pink.r - white.r));
    g = Math.round(white.g + t * (pink.g - white.g));
    b = Math.round(white.b + t * (pink.b - white.b));
  }
  return `rgb(${r},${g},${b})`;
}

/**
 * Format a stat value for display.
 */
function formatStatValue(value, format) {
  if (value === null || value === undefined || value === '') return '—';
  const n = parseFloat(value);
  if (isNaN(n)) return '—';
  switch (format) {
    case 'int':  return Math.round(n).toString();
    case 'avg':  return n.toFixed(3).replace(/^0\./, '.');
    case 'ops':  return n.toFixed(3);
    case 'era':  return n.toFixed(2);
    case 'dec1': return n.toFixed(1);
    case 'dec2': return n.toFixed(2);
    case 'pct':  return (n * 100).toFixed(1) + '%';
    default:     return value.toString();
  }
}
