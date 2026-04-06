/**
 * Network Playbook — Unified cross-client routing intelligence UI.
 * Replaces the old 7-tab network analysis interface with a single
 * unified playbook that pools all client data for higher confidence.
 */
const NetworkAnalysis = {
  _cache: null,
  _filter: null,

  async render() {
    const el = document.getElementById('mainContent');
    if (!el) return;

    el.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
          <h2 style="margin:0;font-size:22px">Network Playbook</h2>
          <p style="margin:4px 0 0;font-size:13px;color:var(--text-secondary)">Cross-client routing rules &mdash; pooled data for higher confidence</p>
        </div>
        <button class="btn btn-primary" onclick="NetworkAnalysis.recompute()">Refresh</button>
      </div>
      <div id="network-playbook-content"><div class="empty-state"><p>Loading...</p></div></div>`;

    await this._fetchPlaybook();
  },

  async _fetchPlaybook() {
    const container = document.getElementById('network-playbook-content');
    if (!container) return;

    try {
      if (this._cache) {
        this._renderPlaybook(container, this._cache);
        return;
      }
      const res = await fetch('/api/network/playbook');
      const data = await res.json();
      if (data && data.error) {
        container.innerHTML = `<div class="empty-state"><h3>Not Computed Yet</h3><p>${data.error}</p><button class="btn btn-primary" onclick="NetworkAnalysis.recompute()">Compute Now</button></div>`;
        return;
      }
      this._cache = data;
      this._renderPlaybook(container, data);
    } catch (err) {
      container.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p></div>`;
    }
  },

  async recompute() {
    const container = document.getElementById('network-playbook-content');
    if (!container) return;
    container.innerHTML = '<div class="empty-state"><p>Computing unified playbook across all clients...</p><div class="spinner"></div></div>';
    this._cache = null;

    try {
      const res = await fetch('/api/network/recompute', { method: 'POST' });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error || 'Recompute failed');
      await this._fetchPlaybook();
    } catch (err) {
      container.innerHTML = `<div class="empty-state"><h3>Recompute Failed</h3><p>${err.message}</p></div>`;
    }
  },

  _renderPlaybook(el, data) {
    if (!data || !data.rows) {
      el.innerHTML = '<div class="empty-state"><h3>No Playbook Data</h3><p>Click Refresh to compute the unified network playbook.</p><button class="btn btn-primary" onclick="NetworkAnalysis.recompute()">Compute Now</button></div>';
      return;
    }

    const rows = data.rows;
    const summary = data.summary;
    let html = '';

    // Summary bar
    html += `<div class="card" style="padding:12px 20px;margin-bottom:12px;display:flex;flex-wrap:wrap;gap:16px;align-items:center">
      <div><span style="font-size:20px;font-weight:700">${summary.totalRows}</span> <span style="font-size:13px;color:var(--text-secondary)">routing rules</span></div>
      <span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#DBEAFE;color:#1E40AF;font-weight:600">Pooled from ${data.clientCount} clients</span>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        ${summary.untested ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#EDE9FE;color:#6D28D9;font-weight:600">' + summary.untested + ' Untested</span>' : ''}
        ${summary.hostile ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FEE2E2;color:#991B1B;font-weight:600">' + summary.hostile + ' Hostile</span>' : ''}
        ${summary.resistant ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FED7AA;color:#9A3412;font-weight:600">' + summary.resistant + ' Resistant</span>' : ''}
        ${summary.viable ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FEF3C7;color:#92400E;font-weight:600">' + summary.viable + ' Viable</span>' : ''}
        ${summary.strong ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#D1FAE5;color:#065F46;font-weight:600">' + summary.strong + ' Strong</span>' : ''}
      </div>
      <div style="display:flex;gap:8px">
        <span style="font-size:11px;color:var(--text-secondary)">${summary.confident} confident</span>
        <span style="font-size:11px;color:var(--text-secondary)">${summary.earlySignal} early signal</span>
        <span style="font-size:11px;color:var(--text-secondary)">${summary.rebillBlockers} rebill blockers</span>
      </div>
      <div style="margin-left:auto"><button class="btn btn-secondary btn-sm" onclick="NetworkAnalysis.exportCsv()">Export CSV</button></div>
    </div>`;

    // Filter buttons
    const filter = this._filter;
    html += `<div style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap">
      ${[
        { key: null, label: 'All', count: rows.length },
        { key: 'UNTESTED', label: 'Untested', count: summary.untested, bg: '#EDE9FE', color: '#6D28D9' },
        { key: 'HOSTILE', label: 'Hostile', count: summary.hostile, bg: '#FEE2E2', color: '#991B1B' },
        { key: 'RESISTANT', label: 'Resistant', count: summary.resistant, bg: '#FED7AA', color: '#9A3412' },
        { key: 'VIABLE', label: 'Viable', count: summary.viable, bg: '#FEF3C7', color: '#92400E' },
        { key: 'STRONG', label: 'Strong', count: summary.strong, bg: '#D1FAE5', color: '#065F46' },
        { key: 'PREPAID', label: 'Prepaid', count: summary.prepaid, bg: '#F5F3FF', color: '#7C3AED' },
      ].map(f => {
        const active = filter === f.key;
        const style = active ? 'background:var(--text);color:white' : f.bg ? 'background:'+f.bg+';color:'+f.color : 'background:var(--bg);color:var(--text);border:1px solid var(--border)';
        return '<button style="font-size:11px;font-weight:600;padding:4px 14px;border-radius:16px;border:none;cursor:pointer;'+style+'" onclick="NetworkAnalysis._filter='+(f.key?"'"+f.key+"'":"null")+';NetworkAnalysis._renderPlaybook(document.getElementById(\'network-playbook-content\'),NetworkAnalysis._cache)">'+f.label+' '+f.count+'</button>';
      }).join('')}
    </div>`;

    // Filter rows
    let filtered = rows;
    if (filter === 'UNTESTED') filtered = rows.filter(r => r.rebillTier.includes('UNTESTED'));
    else if (filter === 'HOSTILE') filtered = rows.filter(r => r.rebillTier.includes('HOSTILE'));
    else if (filter === 'RESISTANT') filtered = rows.filter(r => r.rebillTier.includes('RESISTANT'));
    else if (filter === 'VIABLE') filtered = rows.filter(r => r.rebillTier.includes('VIABLE'));
    else if (filter === 'STRONG') filtered = rows.filter(r => r.rebillTier.includes('STRONG'));
    else if (filter === 'PREPAID') filtered = rows.filter(r => r.isPrepaid);

    // Render cards
    if (filtered.length > 0) {
      const colCount = Math.max(1, Math.min(4, Math.floor((el.offsetWidth || 900) / 320)));
      const cols = Array.from({ length: colCount }, () => []);
      for (let i = 0; i < filtered.length; i++) {
        cols[i % colCount].push(filtered[i]);
      }
      html += '<div style="display:flex;gap:12px;align-items:flex-start">';
      for (const col of cols) {
        html += '<div style="flex:1;min-width:0">';
        for (const row of col) {
          html += this._playbookCard(row);
        }
        html += '</div>';
      }
      html += '</div>';
    } else {
      html += '<div class="empty-state"><h3>No matching rules</h3></div>';
    }

    el.innerHTML = html;
  },

  _agreementBadge(agreement) {
    if (!agreement || agreement.total === 0) return '';
    const ratio = agreement.agree / agreement.total;
    const color = ratio >= 1 ? '#065F46' : ratio >= 0.5 ? '#92400E' : '#991B1B';
    const bg = ratio >= 1 ? '#D1FAE5' : ratio >= 0.5 ? '#FEF3C7' : '#FEE2E2';
    const label = ratio >= 1 ? 'All agree' : `${agreement.agree}/${agreement.total} agree`;
    return `<span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${bg};color:${color};font-weight:600">${label}</span>`;
  },

  _agreementDetail(agreement) {
    if (!agreement || !agreement.perClient || agreement.perClient.length === 0) return '';
    const allAgree = agreement.perClient.every(c => c.agrees);
    if (allAgree) return '';
    let html = '<div style="margin-top:4px;padding:6px 8px;border-radius:4px;background:var(--bg);border:1px solid var(--border)">';
    html += '<div style="font-size:9px;font-weight:600;color:var(--text-secondary);margin-bottom:3px">PER-CLIENT BREAKDOWN</div>';
    for (const c of agreement.perClient) {
      const color = c.agrees ? '#065F46' : '#991B1B';
      const icon = c.agrees ? '&#10003;' : '&#10007;';
      html += `<div style="font-size:10px;color:${color}">${icon} ${c.clientName}: ${c.best} ${c.rate}%${c.att ? ' (' + c.att + ' att)' : ''}</div>`;
    }
    html += '</div>';
    return html;
  },

  _playbookCard(r) {
    const baseTier = r.rebillTier.replace('Early: ', '');
    const tierBg = baseTier === 'UNTESTED' ? '#EDE9FE' : baseTier === 'HOSTILE' ? '#FEE2E2' : baseTier === 'RESISTANT' ? '#FED7AA' : baseTier === 'VIABLE' ? '#FEF3C7' : '#D1FAE5';
    const tierColor = baseTier === 'UNTESTED' ? '#6D28D9' : baseTier === 'HOSTILE' ? '#991B1B' : baseTier === 'RESISTANT' ? '#9A3412' : baseTier === 'VIABLE' ? '#92400E' : '#065F46';
    const confBg = r.confidenceTier === 'Confident' ? '#D1FAE5' : '#FEF3C7';
    const confColor = r.confidenceTier === 'Confident' ? '#065F46' : '#92400E';
    const expandId = 'npb_' + Math.random().toString(36).slice(2, 8);
    const copyId = 'npbc_' + Math.random().toString(36).slice(2, 8);
    const binText = (r.bins || []).join(', ');

    let html = `<div class="card" style="margin-bottom:12px;padding:0;border-left:4px solid ${tierColor};overflow:hidden">`;

    // ── HEADER ──
    html += `<div style="padding:16px 20px 12px;display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
      <div style="min-width:0">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <span style="font-size:15px;font-weight:600">${r.issuer_bank}</span>
          ${r.isPrepaid ? '<span style="font-size:9px;padding:2px 6px;border-radius:3px;background:#EDE9FE;color:#6D28D9;font-weight:600">PREPAID</span>'
            : ''}
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${tierBg};color:${tierColor};font-weight:600">${r.rebillTier}</span>
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${confBg};color:${confColor};font-weight:600">${r.confidenceTier}</span>
          ${r.isRebillBlocker ? '<span style="font-size:9px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B;font-weight:600">REBILL BLOCKER</span>' : ''}
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:#DBEAFE;color:#1E40AF;font-weight:600">${r.clientCount} client${r.clientCount !== 1 ? 's' : ''}</span>
          ${this._agreementBadge(r.initialAgreement)}
        </div>
        <div style="font-size:11px;color:var(--text-secondary);margin-top:4px">${r.acquired} customers | ${r.binCount} BINs | ${(r.clientNames || []).join(', ')}</div>
      </div>
      <button id="${copyId}" onclick="navigator.clipboard.writeText('${binText.replace(/'/g, "\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy BINs',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:10px;cursor:pointer;padding:3px 10px;flex-shrink:0">Copy BINs</button>
    </div>`;

    // ── 4 METRIC BOXES ──
    const initTop = r.initialBest[0];
    const upsTop = r.upsellBest[0];
    const cascTop = r.cascadeChain[0];
    html += `<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:0 20px 12px">`;

    // Box 1: Initial
    html += `<div style="background:#E1F5EE;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#065F46;font-weight:600">INITIAL ${this._agreementBadge(r.initialAgreement)}</div>
      <div style="font-size:13px;font-weight:600;color:#065F46;margin-top:2px">${initTop ? initTop.processor + ' ' + initTop.rate + '%' : 'no data'}</div>
      <div style="font-size:9px;color:#065F46">${initTop ? '(' + initTop.app + '/' + initTop.att + ')' : ''}</div>
    </div>`;

    // Box 2: Cascade
    html += `<div style="background:#EDE9FE;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#6D28D9;font-weight:600">CASCADE</div>
      <div style="font-size:12px;font-weight:500;color:#6D28D9;margin-top:2px">${r.cascadeChain.length > 0 ? r.cascadeChain.map(c => c.name).join(' &rarr; ') : 'no data'}</div>
      <div style="font-size:9px;color:#6D28D9">${cascTop ? cascTop.rate + '% save rate' : ''}</div>
    </div>`;

    // Box 3: Upsell
    html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#5F5E5A;font-weight:600">UPSELL ${this._agreementBadge(r.upsellAgreement)}</div>
      <div style="font-size:13px;font-weight:500;color:var(--text);margin-top:2px">${upsTop ? upsTop.processor + ' ' + upsTop.rate + '%' : 'no data'}</div>
      <div style="font-size:9px;color:#5F5E5A">${upsTop ? '(' + upsTop.app + '/' + upsTop.att + ')' : ''}</div>
    </div>`;

    // Box 4: Rebill
    const rebTop = r.rebillBest[0];
    if (baseTier === 'UNTESTED') {
      html += `<div style="background:#EDE9FE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#6D28D9;font-weight:600">REBILL — UNTESTED</div>
        <div style="font-size:11px;font-weight:600;color:#6D28D9;margin-top:2px">0% at $97.48</div>
        <div style="font-size:9px;color:#6D28D9">Test price drop (${r.c1.att} att)</div>
      </div>`;
    } else if (r.c1.att === 0) {
      html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#5F5E5A;font-weight:600">REBILL</div>
        <div style="font-size:11px;color:#5F5E5A;margin-top:2px">No rebill data</div>
      </div>`;
    } else {
      html += `<div style="background:${tierBg};border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:${tierColor};font-weight:600">REBILL C1: ${r.c1.rate}% ${this._agreementBadge(r.rebillAgreement)}</div>
        <div style="font-size:12px;font-weight:500;color:${tierColor};margin-top:2px">${rebTop ? rebTop.processor + ' ' + rebTop.c1_rate + '%' : 'no data'}</div>
        <div style="font-size:9px;color:${tierColor}">C2: ${r.c2.rate}% (${r.c2.app}/${r.c2.att})</div>
      </div>`;
    }

    html += '</div>';

    // ── EXPAND TRIGGER ──
    html += `<div onclick="var e=document.getElementById('${expandId}');e.style.display=e.style.display==='none'?'':'none'" style="border-top:1px dashed var(--border);padding:6px 0;text-align:center;cursor:pointer">
      <span style="font-size:11px;color:var(--text-muted)">&#9660; Details</span>
    </div>`;

    // ── EXPANDED DETAILS ──
    html += `<div id="${expandId}" style="display:none;padding:0 20px 16px;border-top:1px solid var(--border)">`;

    // Section: Initial routing
    html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Initial Routing ${this._agreementBadge(r.initialAgreement)}</div>`;
    if (r.initialBest.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.initialBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#D1FAE5;color:#065F46">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
      }
      html += '</div>';
    }
    if (r.initialBlock.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
      for (const p of r.initialBlock) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att} att)</span>`;
      }
      html += '</div>';
    }
    html += this._agreementDetail(r.initialAgreement);
    html += '</div>';

    // Section: Cascade
    if (r.cascadeChain.length > 0 || r.cascadeOn.length > 0 || r.cascadeSkip.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Cascade Chain</div>`;
      if (r.cascadeChain.length > 0) {
        html += '<div style="font-size:12px;margin-bottom:4px">' + r.cascadeChain.map(c => `<span style="font-weight:500">${c.name}</span> ${c.rate}%`).join(' &rarr; ') + '</div>';
      }
      if (r.cascadeOn.length > 0) {
        html += '<div style="font-size:11px;color:#065F46;margin-bottom:2px">Cascade on: ' + r.cascadeOn.map(d => d.reason + ' (' + d.recoveryRate + '% recovery)').join(', ') + '</div>';
      }
      if (r.cascadeSkip.length > 0) {
        html += '<div style="font-size:11px;color:#991B1B">Skip: ' + r.cascadeSkip.map(d => d.reason).join(', ') + '</div>';
      }
      html += '</div>';
    }

    // Section: Upsell
    if (r.upsellBest.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Upsell Routing ${this._agreementBadge(r.upsellAgreement)}</div>`;
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
      for (const p of r.upsellBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#F1EFE8;color:#5F5E5A">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
      }
      html += '</div>';
      html += this._agreementDetail(r.upsellAgreement);
      html += '</div>';
    }

    // Section: Rebill
    html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Rebill Routing &mdash; ${r.rebillTier} ${this._agreementBadge(r.rebillAgreement)}</div>`;
    html += `<div style="font-size:12px;margin-bottom:6px">C1: <strong>${r.c1.rate}%</strong> (${r.c1.app}/${r.c1.att}) | C2: <strong>${r.c2.rate}%</strong> (${r.c2.app}/${r.c2.att})</div>`;
    if (r.rebillBest.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.rebillBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#D1FAE5;color:#065F46">${p.processor} C1:${p.c1_rate}% (${p.c1_app}/${p.c1_att})</span>`;
      }
      html += '</div>';
    }
    if (r.rebillBlock.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.rebillBlock) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.total_att} att)</span>`;
      }
      html += '</div>';
    }
    html += this._agreementDetail(r.rebillAgreement);
    if (r.priceStrategy) {
      const ps = r.priceStrategy;
      const psBg = ps.tier === 'UNTESTED' ? '#EDE9FE' : ps.tier === 'HOSTILE' ? '#FEE2E2' : '#FEF3C7';
      const psColor = ps.tier === 'UNTESTED' ? '#6D28D9' : ps.tier === 'HOSTILE' ? '#991B1B' : '#92400E';
      const psBorder = ps.tier === 'UNTESTED' ? '#DDD6FE' : ps.tier === 'HOSTILE' ? '#FECACA' : '#FDE68A';
      html += `<div style="margin-top:6px;padding:8px 10px;border-radius:6px;background:${psBg};border:1px solid ${psBorder}">
        <div style="font-size:10px;font-weight:700;color:${psColor};margin-bottom:4px">PRICE STRATEGY &mdash; ${ps.tier}</div>
        <div style="font-size:12px;color:${psColor}">${ps.recommendation}</div>`;
      if (ps.scenarios) {
        for (const s of ps.scenarios) {
          if (s.price) {
            html += `<div style="font-size:10px;color:${psColor};margin-top:2px;margin-left:8px">At $${s.price}: need ${s.breakEvenRate}% to break even (2x current = $${s.rpaAt2x} RPA)</div>`;
          } else {
            html += `<div style="font-size:10px;color:${psColor};margin-top:2px;margin-left:8px">${s.label}: ${s.rate}% &rarr; $${s.rpa.toFixed(2)} RPA</div>`;
          }
        }
      }
      html += '</div>';
    }
    if (r.isPrepaid) {
      html += '<div style="font-size:11px;color:#6D28D9;margin-top:2px">Prepaid &mdash; no CPA cost, any rebill is pure margin</div>';
    }
    html += '</div>';

    // Section: Salvage
    if (r.salvageSequence.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Rebill Salvage Sequence</div>`;
      for (const s of r.salvageSequence) {
        if (s.isStop) {
          html += `<div style="font-size:11px;color:#991B1B;margin-bottom:2px">Att ${s.attempt}: <strong>STOP</strong> &mdash; ${s.stopMessage}</div>`;
        } else {
          html += `<div style="font-size:11px;margin-bottom:2px">Att ${s.attempt}: <strong>${s.processor}</strong> ${s.rate}% &middot; RPA $${s.rpa}</div>`;
        }
      }
      if (r.rebillRetryOn.length > 0) {
        html += '<div style="font-size:11px;color:#065F46;margin-top:4px">Retry on: ' + r.rebillRetryOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
      }
      if (r.rebillStopOn.length > 0) {
        html += '<div style="font-size:11px;color:#991B1B;margin-top:2px">Stop on: ' + r.rebillStopOn.map(d => d.reason).join(', ') + '</div>';
      }
      html += '</div>';
    }

    // Section: Lifecycle
    if (r.acquisitionAffinity.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Lifecycle Context</div>`;
      html += '<div style="display:flex;flex-wrap:wrap;gap:6px">';
      for (const a of r.acquisitionAffinity) {
        const bg = a.rebRate >= 20 ? '#D1FAE5' : a.rebRate >= 10 ? '#FEF3C7' : '#FEE2E2';
        const color = a.rebRate >= 20 ? '#065F46' : a.rebRate >= 10 ? '#92400E' : '#991B1B';
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:${bg};color:${color}">Acq ${a.processor} &rarr; ${a.rebRate}% rebill (${a.rebApp}/${a.rebAtt})</span>`;
      }
      html += '</div></div>';
    }

    // Section: L4 Sub-Groups
    if (r.l4Groups && r.l4Groups.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Card Type Routing</div>`;
      for (const o of r.l4Groups) {
        const label = [o.card_brand, o.is_prepaid ? 'Prepaid' : '', o.card_type].filter(Boolean).join(' ');
        const olCopyId = 'nol_' + Math.random().toString(36).slice(2, 8);
        const olBins = (o.bins || []).join(', ');
        const lvlBg = o.routingLevel === 'own' ? '#D1FAE5' : o.routingLevel === 'partial' ? '#FEF3C7' : '#F1EFE8';
        const lvlColor = o.routingLevel === 'own' ? '#065F46' : o.routingLevel === 'partial' ? '#92400E' : '#5F5E5A';
        const lvlLabel = o.routingLevel === 'own' ? 'Own routing' : o.routingLevel === 'partial' ? 'Partial data' : 'Use bank fallback';
        let signals = [];
        if (o.isInitOutlier) {
          const arrow = o.initDelta > 0 ? '&#9650;' : '&#9660;';
          const sigColor = o.initDelta > 0 ? '#065F46' : '#991B1B';
          signals.push(`<span style="color:${sigColor}">Init ${o.initRate}% (${arrow}${Math.abs(o.initDelta)}pp) <span style="font-size:9px;color:var(--text-muted)">(${o.initApp}/${o.initAtt})</span></span>`);
        }
        if (o.isC1Outlier) {
          const arrow = o.c1Delta > 0 ? '&#9650;' : '&#9660;';
          const sigColor = o.c1Delta > 0 ? '#065F46' : '#991B1B';
          signals.push(`<span style="color:${sigColor}">C1 ${o.c1Rate}% (${arrow}${Math.abs(o.c1Delta)}pp) <span style="font-size:9px;color:var(--text-muted)">(${o.c1App}/${o.c1Att})</span></span>`);
        }

        html += `<div style="padding:8px;margin-bottom:6px;border:1px solid var(--border);border-radius:6px;background:var(--bg)">`;
        html += `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px">
          <span style="font-weight:600;font-size:12px">${label}</span>
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${lvlBg};color:${lvlColor};font-weight:600">${lvlLabel}</span>
          <span style="color:var(--text-muted);font-size:10px">${o.bin_count} BINs</span>
          ${signals.length > 0 ? '<span style="font-size:10px">' + signals.join(' &middot; ') + '</span>' : ''}
          <button id="${olCopyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${olBins.replace(/'/g, "\\'")}');var b=document.getElementById('${olCopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:9px;cursor:pointer;padding:1px 6px;margin-left:auto">Copy BINs</button>
        </div>`;

        if (o.routingLevel !== 'fallback') {
          if (o.initRouting && o.initRouting.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">INIT</span>';
            for (const p of o.initRouting) {
              const pbg = p.app > 0 ? '#D1FAE5' : '#FEE2E2';
              const pcolor = p.app > 0 ? '#065F46' : '#991B1B';
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:${pbg};color:${pcolor}">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
            }
            for (const p of (o.initBlock || [])) {
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att})</span>`;
            }
            html += '</div>';
          }
          if (o.cascadeTargets && o.cascadeTargets.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">CASC</span>';
            html += '<span style="font-size:10px">' + o.cascadeTargets.map(c => `<span style="padding:2px 6px;border-radius:3px;background:#EDE9FE;color:#6D28D9">${c.name} ${c.rate}%</span>`).join(' &rarr; ') + '</span>';
            html += '</div>';
          }
          if (o.cascadeOn && o.cascadeOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#065F46">Cascade on: ' + o.cascadeOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
          }
          if (o.cascadeSkip && o.cascadeSkip.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#991B1B">Skip: ' + o.cascadeSkip.map(d => d.reason).join(', ') + '</div>';
          }
          if (o.rebillRouting && o.rebillRouting.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">REBILL</span>';
            for (const p of o.rebillRouting) {
              const pbg = p.app > 0 ? '#D1FAE5' : '#FEE2E2';
              const pcolor = p.app > 0 ? '#065F46' : '#991B1B';
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:${pbg};color:${pcolor}">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
            }
            for (const p of (o.rebillBlock || [])) {
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att})</span>`;
            }
            html += '</div>';
          }
          if (o.salvageSequence && o.salvageSequence.length > 0 && !o.salvageSequence[0].isStop) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">SALVAGE</span>';
            for (const s of o.salvageSequence) {
              if (s.isStop) {
                html += `<span style="font-size:10px;color:#991B1B">Att ${s.attempt}: STOP &mdash; ${s.stopMessage}</span>`;
              } else {
                html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEF3C7;color:#92400E">Att ${s.attempt}: ${s.processor} ${s.rate}% &middot; $${s.rpa} RPA</span>`;
              }
            }
            html += '</div>';
          }
          if (o.rebillRetryOn && o.rebillRetryOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#065F46">Retry on: ' + o.rebillRetryOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
          }
          if (o.rebillStopOn && o.rebillStopOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#991B1B">Stop on: ' + o.rebillStopOn.map(d => d.reason).join(', ') + '</div>';
          }
        }

        html += '</div>';
      }
      html += '</div>';
    }

    html += '</div>'; // end expanded
    html += '</div>'; // end card
    return html;
  },

  exportCsv() {
    if (!this._cache || !this._cache.rows) return;
    const rows = this._cache.rows;
    const esc = (s) => '"' + String(s || '').replace(/"/g, '""') + '"';
    const fmtProcs = (ps) => ps.map(p => p.processor + ' ' + p.rate + '% (' + p.app + '/' + p.att + ')').join('; ');

    const headers = ['Bank', 'Prepaid', 'Clients', 'Acquired', 'BIN Count', 'Confidence', 'Rebill Tier',
      'Initial Best', 'Cascade Chain', 'Upsell Best',
      'C1 Rate', 'C1 Att', 'C2 Rate', 'C2 Att',
      'Rebill Best', 'Salvage', 'Price Strategy',
      'Acq Affinity', 'Agreement'];
    const csvRows = [headers.join(',')];

    for (const r of rows) {
      const cascStr = r.cascadeChain.map(c => c.name + ' ' + c.rate + '%').join(' > ');
      const salvStr = r.salvageSequence.filter(s => !s.isStop).map(s => 'Att ' + s.attempt + ': ' + s.processor + ' ' + s.rate + '% $' + s.rpa + ' RPA').join('; ');
      const acqStr = r.acquisitionAffinity.map(a => a.processor + ' > ' + a.rebRate + '%').join('; ');
      const agreeStr = r.initialAgreement ? r.initialAgreement.agree + '/' + r.initialAgreement.total + ' agree' : '';

      csvRows.push([
        esc(r.issuer_bank), r.isPrepaid ? 'Yes' : 'No',
        r.clientCount, r.acquired, r.binCount,
        esc(r.confidenceTier), esc(r.rebillTier),
        esc(fmtProcs(r.initialBest)),
        esc(cascStr),
        esc(fmtProcs(r.upsellBest)),
        r.c1.rate, r.c1.att, r.c2.rate, r.c2.att,
        esc(r.rebillBest.map(p => p.processor + ' C1:' + p.c1_rate + '%').join('; ')),
        esc(salvStr),
        esc(r.priceStrategy ? r.priceStrategy.recommendation : ''),
        esc(acqStr),
        esc(agreeStr),
      ].join(','));
    }

    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'network-playbook.csv';
    a.click();
    URL.revokeObjectURL(url);
  },
};
