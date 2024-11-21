# Foreign country codes which are not canton abbreviations at the same time or "CH"
foreign_country_codes = [
    'AD', 'AE', 'AF', 'AL', 'AM', 'AO', 'AQ', 'AS', 'AT', 'AU', 'AW', 'AX', 'AZ',
    'BA', 'BB', 'BD', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BT', 'BV', 'BW', 'BY', 'BZ',
    'CA', 'CC', 'CD', 'CF', 'CG', 'CI', 'CK', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ',
    'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ',
    'EC', 'EE', 'EG', 'EH', 'EL', 'ER', 'ES', 'ET',
    'FI', 'FJ', 'FK', 'FM', 'FO',
    'GA', 'GB', 'GD', 'GF', 'GG', 'GH', 'GI', 'GM', 'GN', 'GP', 'GQ', 'GS', 'GT', 'GU', 'GW', 'GY',
    'HK', 'HM', 'HN', 'HR', 'HT', 'HU',
    'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR', 'IS', 'IT',
    'JE', 'JM', 'JO', 'JP',
    'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ',
    'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LV', 'LY',
    'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ',
    'NC', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU', 'NZ',
    'OM',
    'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 'PT', 'PW', 'PY',
    'QA',
    'RE', 'RO', 'RS', 'RU', 'RW',
    'SA', 'SB', 'SC', 'SD', 'SE', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SR', 'SS', 'ST', 'SV', 'SX', 'SY',
    'TC', 'TD', 'TF', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ',
    'UA', 'UG', 'UM', 'US', 'UY', 'UZ',
    'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU',
    'WF', 'WS',
    'YE', 'YT',
    'ZA', 'ZM', 'ZW',
    'XK', 'FRA', 'USA', 'UK', 'BGR', 'BIH', 'NA', 'XZ', 'CHN', 'DEU', 'GBR', 'HKD', 'POL', 'PRT', 'RKS'
]


# Foreign country names (only used in French entries)
additional_foreign_indicators = [
    'Afrique du Sud', 'Albanie', 'Algérie', 'Allemagne', 'Andorre', 'Angola', 'Arabie Saoudite', 'Arabie Saoutite',
    'Argentine', 'Arménie', 'Australie', 'Austriche', 'Azerbaïdjan', 'Bahamas', 'Bahreïn',
    'Bangladesh', 'Barbade', 'Belgique', 'Bénin', 'Bhoutan', 'Biélorussie', 'Birmanie', 'Bolivie',
    'Bosnie-Herzégovine', 'Botswana', 'Brésil', 'Brunei', 'Bulgarie', 'Burkina Faso', 'Burundi',
    'Cambodge', 'Cameroun', 'Canada', 'Cap-Vert', 'Chili', 'Chine', 'Chypre', 'Colombie',
    'Comores', 'Congo-Brazzaville', 'Congo-Kinshasa', 'Corée du Nord', 'Corée du Sud',
    'Costa Rica', "Côte d'Ivoire", 'Croatie', 'Cuba', 'Danemark', 'Djibouti', 'Dominique',
    'Égypte', 'Émirats arabes unis', 'Emirat arabes unis', 'Équateur', 'Érythrée', 'Espagne', 'Estonie', 'États-Unis',
    'Éthiopie', 'Finlande', 'France', 'Gabon', 'Gambie', 'Géorgie', 'Ghana', 'Grèce',
    'Grenade', 'Guatemala', 'Guinée', 'Guinée-Bissau', 'Guinée équatoriale', 'Guyana', 'Haïti',
    'Honduras', 'Hongrie', 'Inde', 'Indonésie', 'Irak', 'Iran', 'Irlande', 'Islande', 'Israël',
    'Italie', 'Jamaïque', 'Japon', 'Jordanie', 'Kazakhstan', 'Kenya', 'Kirghizistan', 'Kiribati',
    'Kosovo', 'Koweït', 'Laos', 'Lesotho', 'Lettonie', 'Liban', 'Libéria', 'Libye',
    'Liechtenstein', 'Lituanie', 'Luxembourg', 'Macédoine du Nord', 'Madagascar', 'Malaisie',
    'Malawi', 'Maldives', 'Mali', 'Malte', 'Maroc', 'Marshall', 'Martinique', 'Maurice', 'Mauritanie',
    'Mexique', 'Micronésie', 'Moldavie', 'Monaco', 'Mongolie', 'Monténégro', 'Mozambique',
    'Namibie', 'Nauru', 'Népal', 'Nicaragua', 'Niger', 'Nigeria', 'Norvège', 'Nouvelle-Zélande',
    'Oman', 'Ouganda', 'Ouzbékistan', 'Pakistan', 'Palaos', 'Palestine', 'Panama',
    'Papouasie-Nouvelle-Guinée', 'Paraguay', 'Pays-Bas', 'Pérou', 'Philippines', 'Pologne',
    'Portugal', 'Qatar', 'République centrafricaine', 'République démocratique du Congo',
    'République dominicaine', 'République tchèque', 'Roumanie', 'Royaume-Uni', 'Russie', 'Rwanda',
    'Saint-Kitts-et-Nevis', 'Saint-Vincent-et-les-Grenadines', 'Sainte-Lucie', 'Saint-Marin',
    'Salomon', 'Salvador', 'Samoa', 'São Tomé-et-Principe', 'Sénégal', 'Serbie', 'Seychelles',
    'Sierra Leone', 'Singapour', 'Slovaquie', 'Slovénie', 'Somalie', 'Soudan', 'Soudan du Sud',
    'Sri Lanka', 'Suède', 'Suisse', 'Suriname', 'Syrie', 'Tadjikistan', 'Taïwan', 'Tanzanie', 'Tchad',
    'Thaïlande', 'Timor oriental', 'Togo', 'Tonga', 'Trinité-et-Tobago', 'Tunisie', 'Turkménistan',
    'Turquie', 'Tuvalu', 'Ukraine', 'Uruguay', 'Vanuatu', 'Vatican', 'Venezuela', 'Viêt Nam',
    'Yémen', 'Zambie', 'Zimbabwe'
]


# Names of foreign municipalities that have similar names to Swiss municipalities
false_positives = [
    'Taggia',
    'Eaubonne',
    'Montanay',
    'Avermes',
    'Serraval',
    'Fegersheim',
    'Bassens',
    'Ecully',
    'Lutterbach',
    'Eschen',
    'La Rochelle',
    'Porto',
    'Châtel',
    'Buc',
    'Alès',
    'Sindelfingen',
    'Champagnole',
    'Berlin',
    'Saint-Ismier',
    'Vers',
    'Luze',
    'Les Clefs',
    'Bassy (FR)',
    'Saint-Ismier'
]


# Old names that are used in some entires
common_mistakes = {
    'Guemligen': 'Muri bei Bern',
    'Sagno': 'Breggia',
    'Edingen': 'Endingen',
    'Ittingen': 'Ittigen',
    'Ebmatingen': 'Maur',
    'Uitikon Waldegg': 'Uitikon',
    'Yverdon-les-BainsStaderini Sara': 'Yverdon-les-Bains',
    'Bré sopra Lugano': 'Lugano',
    'St.Gallen': 'St. Gallen',
    'Oberhofen am Thunersee Verwaltungsratsmitglied': 'Oberhofen am Thunersee'
}


CONFIG = {
    'foreign_country_codes': foreign_country_codes,
    'additional_foreign_indicators': additional_foreign_indicators,
    'false_positives': false_positives,
    'common_mistakes': common_mistakes
}
