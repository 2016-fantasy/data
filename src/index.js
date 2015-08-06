import fs from 'fs';

import csv from 'csv';
import _ from 'lodash';
import promise from 'promise-callback';


processCandidates()
  .then(processParties)
  .catch(error => console.error('Error', error.stack));

function processCandidates() {
  // http://www.fec.gov/finance/disclosure/metadata/metadataforcandidatesummary.shtml
  // Provide a mapping from the FEC's column names to ours (the 'to' field)
  const mapping = {
    'lin_ima': {to: 'url',   name: 'Link to list of reports for the committee', description: 'List of all disclosure filings for this committee'},
    'can_id':  {to: 'fecId', name: 'Candidate Id'},
    'can_nam': {to: 'name',  name: 'Candidate Name'},
    'can_off': {to: 'office',name: 'Candidate Office'},
    'can_off_sta': {to: 'officeState', name: 'Candidate Office State'},
    'can_off_dis': {to: 'officeDistrict', name: 'Candidate Office District'},
    'can_par_aff': {to: 'party', name: 'Candidate Party Affiliation'},
    'can_inc_cha_ope_sea': {to: 'incumbentChallengerOpenSeat', name: 'Candidate Incumbent Challenger Open Seat'},
    'can_str1': {to: 'street1', name: 'Candidate Street 1'},
    'can_str2': {to: 'street2', name: 'Candidate Street 2'},
    'can_cit':  {to: 'city', name: 'Candidate City'},
    'can_sta':  {to: 'state',name: 'Candidate State'},
    'can_zip':  {to: 'zip',  name: 'Candidate Zip'},
    'ind_ite_con': {to: 'itemizedContributions', name: 'Individual Itemized Contribution'},
    'ind_uni_con': {to: 'unitemizedContributions', name: 'Individual Unitemized Contribution'},
    'ind_con': {to: 'individualContributions', name: 'Individual Contribution'},
    'par_com_con': {to: 'partyCommitteeContribution', name: 'Party Committee Contribution'},
    // 'oth_com_con': '?',
    // 'can_con': '?',
    'tot_con': {to:'totalContributions', name: 'Total Contributions'}
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

  // After mapping names, each name will have their value transformed
  // with the following functions
  const transforms = {
    // This parsing needs to be better. Some names have DR. MR. MRS, etc
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
    },

    'totalContributions': value => {
      const [full, dollars, cents] = value.replace(/,/g, '')
                           .replace(/\$/g, '')
                           .match(/^(\d+)\.(\d+)$/);

      return parseInt(dollars) * 100 + parseInt(cents);
    },

    'party': (() => {
      const mapping = {
        'REP': 'Republican',
        'IND': 'Independent',
        'DEM': 'Democrat',
        'UNK': 'Unknown',
        'OTH': 'Other',
        'LRU': 'La Raza Unida',
        'W':   'Write-In',
        'NNE': 'None',
        'UNI': 'United',
        'IAP': 'Independent American',
        'IDP': 'Independence',
        'UN':  'Unaffiliated',
        'AMP': 'American',
        'NPA': 'No Party Affiliation',
        'GRE': 'Green',
        'FED': 'Federalist',
        'CON': 'Constitution',
        'LIB': 'Libertarian',
        'CST': 'Constitutional',
        'REF': 'Reform',
        'NON': 'Non-Party',
        'CRV': 'Conservative',
        'DFL': 'Democratic-Farmer-Labor',
        'ACE': 'Ace',
        'AIP': 'American Independent',
        'HEL': 'HEL',
        'AME': 'AME',
        'A99': 'A99',
        'JCN': 'Jewish/Christian National',
        'COM': 'Communist',
        'NBC': 'NBC',
        'PCH': 'Personal Choice'
      };

      return value => mapping[value] || value;
    })()
  };

  return promise(fs.readFile, `${__dirname}/../datasets/raw/fec/CandidateSummaryAction.csv`)
    .then(buffer => promise(csv.parse, buffer.toString(), {columns: true, relax: true}))
    .then(processRecords)
    .then(writeCandidates)
    .then(candidates => {console.log('Candidates Done!'); return candidates; })
    .catch(error => console.error('Error', error.stack));

  function processRecords(records) {
    return  _.map(records, transformRecord);

    function transformRecord(record) {
      return _.mapValues(
              _.omit(
                _.mapKeys(record, mapKeys),
                (value, key) => value === null || value === undefined || value === ''
              ),
              mapValues
            );

      function mapKeys(value, key) {
        return (mapping[key] || {to: key}).to;
      }

      function mapValues(value, key) {
        const transform = transforms[key];

        return transform ? transform(value) : value;
      }
    }
  }

  function writeCandidates(records) {
    return promise(fs.writeFile, `${__dirname}/../datasets/processed/candidates.json`, JSON.stringify({candidates: records}, null, '  '))
            .then(() => records);
  }
}

function processParties(candidates) {
  const parties = extractParties(candidates);

  return promise(fs.writeFile, `${__dirname}/../datasets/processed/parties.json`, JSON.stringify({parties}, null, '  '));

  function extractParties(candidates) {
    const parties = _.groupBy(candidates, 'party');


    return _.map(parties, (candidates, name) => ({name, candidates: candidates.length}));
    // return _.mapValues(parties, (candidates, party) => ({candidates: candidates.length}));
  }
}