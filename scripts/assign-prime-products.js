const http = require('http');

function put(productId, group, type) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({ product_group: group, product_type: type });
    const req = http.request({
      hostname: 'localhost', port: 3000,
      path: '/api/products/2/' + productId,
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => resolve({ productId, status: res.statusCode }));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function getType(name) {
  const n = name.toLowerCase();
  if (n.includes('init')) return 'initial';
  if (n.includes('- eot s1') || n.includes('- eot s2')) return 'initial';
  if (n.includes('rcvry') || n.includes('recovery') || n.includes('reprocess')) return 'rebill';
  if (/c\d/.test(n) || /c\d\+/.test(n) || /rebill/i.test(n)) return 'rebill';
  if (n.includes('ots') || n.includes(' ss ') || n.endsWith(' ss') || n.includes('ss (')) return 'initial';
  if (n.includes('subs') && !/c\d/.test(n)) return 'initial';
  if (n.includes('- eot') && !n.includes('s1') && !n.includes('s2')) return 'initial';
  return 'straight_sale';
}

function getGroup(name) {
  const n = name.toLowerCase();

  // Aevéa Skin
  if (/aev.a skin serum/i.test(name)) return 'Aevéa Skin Serum';
  if (/aev.a skin cream/i.test(name)) return 'Aevéa Skin Cream';
  if (/aev.a skin vi?tc/i.test(name)) return 'Aevéa Skin VitC';
  if (/aev.a skin toner/i.test(name)) return 'Aevéa Skin Toner';
  if (/aev.a skin tag/i.test(name)) return 'Aevéa Skin Tag Remover';
  if (/aev.a skin ship/i.test(name)) return 'Shipping & Insurance';

  // Glo Vous Derm
  if (n.includes('glo vous derm') && n.includes('serum')) return 'Glo Vous Derm Serum';
  if (n.includes('glo vous derm') && n.includes('cream')) return 'Glo Vous Derm Cream';
  if (n.includes('glo vous derm') && n.includes('vitc')) return 'Glo Vous Derm VitC';

  // Derm La Fleur
  if (n.includes('derm la fleur') && n.includes('serum')) return 'Derm La Fleur Serum';
  if (n.includes('derm la fleur') && n.includes('cream')) return 'Derm La Fleur Cream';
  if (n.includes('derm la fleur') && n.includes('vitamin')) return 'Derm La Fleur VitC';

  // Derm Le Veux
  if (n.includes('derm le veux') && n.includes('serum')) return 'Derm Le Veux Serum';
  if (n.includes('derm le veux') && n.includes('cream')) return 'Derm Le Veux Cream';
  if (n.includes('derm le veux') && n.includes('vitamin')) return 'Derm Le Veux VitC';

  // Derm Lumière
  if (n.includes('derm lumi') && n.includes('serum')) return 'Derm Lumière Serum';
  if (n.includes('derm lumi') && n.includes('cream')) return 'Derm Lumière Cream';
  if (n.includes('derm lumi') && n.includes('toner')) return 'Derm Lumière Toner';

  // DermLeMar / Derm Le Mar
  if ((n.includes('dermlemar') || n.includes('derm le mar')) && n.includes('gummies')) return 'DermLeMar Gummies';
  if ((n.includes('dermlemar') || n.includes('derm le mar')) && n.includes('serum')) return 'DermLeMar Serum';
  if ((n.includes('dermlemar') || n.includes('derm le mar')) && n.includes('cream')) return 'DermLeMar Cream';

  // Exfolie
  if (n.includes('exfolie anti')) return 'Exfolie Anti Aging';
  if (n.includes('exfolie eye')) return 'Exfolie Eye Cream';
  if (n.includes('exfolie face')) return 'Exfolie Face Serum';
  if (n.includes('exfolie skin toner')) return 'Exfolie Skin Toner';
  if (n.includes('exfolie vit c')) return 'Exfolie Vit C';

  // Reneaux
  if (n.includes('reneaux face')) return 'Reneaux Face Serum';
  if (n.includes('reneaux eye')) return 'Reneaux Eye Cream';

  // MaxxRyze
  if (n.includes('maxxryze') && (n.includes(' me ') || n.includes(' me c') || n.includes('me s1') || n.includes('male enhancement') || n.includes('me gummies'))) return 'MaxxRyze Pro ME';
  if (n.includes('maxxryze') && n.includes('pre')) return 'MaxxRyze Pro Pre-W';
  if (n.includes('maxxryze') && n.includes('post')) return 'MaxxRyze Pro Post-W';
  if (n.includes('maxxryze') && (n.includes('testo') || n.includes('testosterone'))) return 'MaxxRyze Pro Testo';
  if (n.includes('maxxryze') && (n.includes('nitric') || n.includes('no2'))) return 'MaxxRyze Pro NO2';
  if (n.includes('maxxryze') && n.includes('shipping')) return 'Shipping & Insurance';

  // E-XceL Prime
  if (n.includes('e-xcel') || n.includes('e xcel')) return 'E-XceL Prime';

  // ME/Pre-W/Testosterone straight sale bottles
  if (n.startsWith('ME Gummies')) return 'ME Gummies SS';
  if (n.includes('pre-workout') || n.includes('pre workout')) return 'Pre-Workout SS';
  if (n.includes('post-workout') && !n.includes('maxxryze')) return 'Post-Workout SS';
  if (n.startsWith('Testosterone')) return 'Testosterone SS';

  // CBDynamax
  if (n.includes('cbdynamax') && n.includes('blood sugar')) return 'CBDynamax Blood Sugar';
  if (n.includes('cbdynamax') && n.includes('turmeric')) return 'CBDynamax Turmeric';
  if (n.includes('cbdynamax') && n.includes('cbd gummies')) return 'CBDynamax CBD Gummies';

  // Ketosci
  if (n.includes('ketosci keto')) return 'Ketosci Keto';
  if (n.includes('ketosci cleanse')) return 'Ketosci Cleanse';
  if (n.includes('ketosci immunity')) return 'Ketosci Immunity';
  if (n.includes('insurance') && n.includes('keto')) return 'Shipping & Insurance';

  // Max Brute
  if (n.includes('max brute no')) return 'Max Brute NO';
  if (n.includes('max brute bcaa')) return 'Max Brute BCAA';
  if (n.includes('max brute') && !n.includes('no') && !n.includes('bcaa')) return 'Max Brute';
  if (n.includes('insurance') && n.includes('max brute')) return 'Shipping & Insurance';

  // Keto capsules
  if (n.includes('keto') && (n.includes('caps') || n.includes('strength') || n.includes('packet') || n.includes('travel'))) return 'Keto Capsules';

  // Electronics
  if (n.includes('dashcam') || n.includes('dash cam')) return 'Dashcam';
  if (n.includes('earpod')) return 'Earpods Pro';
  if (n.includes('fitness watch') || n.includes('fitness tracker')) return 'Fitness Watch';
  if (n.includes('mini flashlight')) return 'Mini Flashlight';
  if (n.includes('flashlight')) return 'Flashlight';
  if (n.includes('gps tag') || n.includes('tag tracker')) return 'GPS Tag Tracker';
  if (n.includes('wi-fi booster') || n.includes('wi fi booster')) return 'Wi-Fi Booster';
  if (n.includes('bluetooth speaker')) return 'Bluetooth Speaker';
  if (n.includes('ems neck')) return 'EMS Neck Massager';
  if (n.includes('ems foot')) return 'EMS Foot Mat';
  if (n.includes('performance chip') || n.includes('fuelsaver') || n.includes('fuel chip')) return 'Performance Chip';
  if (n.includes('spycam')) return 'Spycam';
  if (n.includes('power bank')) return 'Power Bank';
  if (n.includes('uv wa')) return 'UV Wand';
  if (n.includes('posture corrector')) return 'Posture Corrector';
  if (n.includes('wireless earbud')) return 'Wireless Earbuds';
  if (n.includes('personal blender')) return 'Personal Blender';
  if (n.includes('blackhead remover')) return 'Blackhead Remover';
  if (n.includes('facial cleanser') || n.includes('face scrubber') || n.includes('silicone face')) return 'Face Scrubber';
  if (n.includes('silicone earbud')) return 'Earbud Case';
  if (n.includes('sd card')) return 'SD Card';
  if (n.includes('earbud')) return 'Earbud Case';

  // Other
  if (name.startsWith('CP-')) return 'Cash Purchase';
  if (name.startsWith('$')) return 'Price Products';
  if (/^\d/.test(name) && !n.includes('gb sd')) return 'Price Products';
  if (n.includes('insurance')) return 'Shipping & Insurance';
  if (n.includes('shipping')) return 'Shipping & Insurance';
  if (n.includes('erecovery')) return 'ERecovery';
  if (n.includes('test product')) return 'Test Products';

  return 'Other';
}

// Fetch products and assign
http.get('http://localhost:3000/api/products/2', res => {
  let data = '';
  res.on('data', c => data += c);
  res.on('end', async () => {
    const products = JSON.parse(data);
    let ok = 0, fail = 0;
    const groupCounts = {};

    for (const p of products) {
      const group = getGroup(p.product_name);
      const type = getType(p.product_name);
      groupCounts[group] = (groupCounts[group] || 0) + 1;

      try {
        const r = await put(p.product_id, group, type);
        if (r.status === 200) ok++;
        else { fail++; console.log('FAIL:', p.product_id, p.product_name, r.status); }
      } catch(e) { fail++; }
    }

    console.log('Done. OK:', ok, 'Failed:', fail);
    console.log('\nGroups created:');
    Object.entries(groupCounts).sort((a,b) => b[1] - a[1]).forEach(([g, c]) => {
      console.log('  (' + c + ')', g);
    });
  });
});
