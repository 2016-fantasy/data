import fs from 'fs';

import csv from 'csv';
import _ from 'lodash';
import promise from 'promise-callback';



const mapping = {
  'lin_ima': 'url',
  'can_id':  'fecId',
  'can_nam': 'name',
  'can_off': 'off?',
  'an_off_sta': 'off_sta?',
  'can_off_dis': 'off_dis?',
  'can_par_aff': 'party',
  'can_inc_cha_ope_sea': '??',
  'can_str1': 'street1',
  'can_str2': 'street2',
  'can_cit': 'city',
  'can_sta': 'state',
  'can_zip': 'zip'//,
  // 'ind_ite_con': '?',
  // 'ind_uni_con': '?',
  // 'ind_con': '?',
  // 'par_com_con': '?',
  // 'oth_com_con': '?',
  // 'can_con': '?',
  // 'tot_con': '?',
  // 'tra_fro_oth_aut_com': '?',
  // 'can_loa': '?',
  // 'oth_loa': '?',
  // 'tot_loa': '?',
  // 'off_to_ope_exp': '?',
  // 'off_to_fun': '?',
  // 'off_to_leg_acc': '?',
  // 'oth_rec': '?',
  // 'tot_rec': '?',
  // 'ope_exp': '?',
  // 'exe_leg_acc_dis': '?',
  // 'fun_dis': '?',
  // 'tra_to_oth_aut_com': '?',
  // 'can_loa_rep': '?',
  // 'oth_loa_rep': '?',
  // 'tot_loa_rep': '?',
  // 'ind_ref': '?',
  // 'par_com_ref': '?',
  // 'oth_com_ref': '?',
  // 'tot_con_ref': '?',
  // 'oth_dis': '?',
  // 'tot_dis': '?',
  // 'cas_on_han_beg_of_per': '?',
  // 'cas_on_han_clo_of_per': '?',
  // 'net_con': '?',
  // 'net_ope_exp': '?',
  // 'deb_owe_by_com': '?',
  // 'deb_owe_to_com': '?',
  // 'cov_sta_dat': '?',
  // 'cov_end_dat': '?'
};


const transforms = {
  'name': name => {
    const parts = name.split(' '),
          [first] = parts,
          last = parts[parts.length - 1];

    let hasSuffix = false;

    if (last.match(/^(JR)|(SR)$/)) hasSuffix = true;

    if (first.endsWith(',')) {
      if (!hasSuffix) parts.push(parts.shift().substr(0, first.length - 1));
      else {
        parts.splice(parts.length - 2, 0, parts.shift().substr(0, first.length - 1)); // Careful, the execution order is crucial here!
      }
    }

    return {
      original: name,
      display: parts.join(' '),
      parts
    };
  }
};

promise(fs.readFile, `${__dirname}/../datasets/raw/fec/CandidateSummaryAction.csv`)
  .then(buffer => promise(csv.parse, buffer.toString(), {columns: true, relax: true}))
  .then(processRecords)
  // .then(showRecords)
  .then(writeRecords)
  .catch(error => console.error('Error', error.stack));

function processRecords(records) {
  return  _.map(records, transformRecord);

  function transformRecord(record) {
    return _.mapValues(
              _.mapKeys(record, mapKeys),
              mapValues);

    function mapKeys(value, key) {
      return mapping[key] || key;
    }

    function mapValues(value, key) {
      const transform = transforms[key];

      return transform ? transform(value) : value;
    }
  }
}

function showRecords(records) {
  console.log(records.length, 'records');
  console.log('Showing first 2...');

  _.each(_.take(records, 2), record => console.log(record));

  return records;
}

function writeRecords(records) {
  return promise(fs.writeFile, `${__dirname}/../datasets/processed/candidates.json`, JSON.stringify({candidates: records}, null, '  '));
}