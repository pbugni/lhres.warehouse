"""Module to test the output of the Mirth Connect channels

NB - this module does NOT provoke actual processing of the test set,
but relies on a persisted copy of the post process database to be
available.  To create said file - run::

    process_testfiles_via_mirth

(and go get some coffee while you wait...)

The test files should not contain ANY sensitive data.  All test files
should either be hand generated from test data, or scrubbed by a process
such as pheme.anonymize

"""
from datetime import datetime
import os
import re
import unittest
from sqlalchemy import select, and_

from pheme.util.config import Config
from pheme.util.pg_access import db_connection
from pheme.warehouse.tables import *
from pheme.warehouse.tests.process_testfiles import MirthInteraction


def setup_module():
    """Populate database with test data for module tests"""

    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")

    "Pull in the filesystem dump from a previous mirth run"
    mi = MirthInteraction()
    mi.restore_database()

    "Run a quick sanity check, whole module requires a populated db"
    connection = db_connection('warehouse')
    count = connection.session.query(HL7_Msh).count()
    connection.disconnect()

    if count < 4000:
        err = "Minimal expected count of records not present. "\
            "Be sure to run 'process_testfiles_via_mirth' as a prerequisite"
        raise RuntimeError(err)


class SQATest(unittest.TestCase):
    """Common SQLAlchemy Database connection management"""

    def setUp(self):
        super(SQATest, self).setUp()
        self.connection = db_connection('warehouse')
        self.session = self.connection.session

    def tearDown(self):
        super(SQATest, self).tearDown()
        self.connection.disconnect()


class TestHl7Msh(SQATest):

    def testMessageControlId(self):
        "Confirm a few known ids made it"
        knownIds = ['04296.6762.70.327884.81.7.6110',
                    '042.974.4.243009.640.167946.62',
                    '043.032.77067.967.56.4636.7304',
                    '0.431.202.76464.03.5.50.91.708',
                    '043.193.87303.07197.642.336.7.',
                    '043.199.72.771707.5.3.702859.7',
                    '043.20145.3.1453.698816.3711.9',
                    '04.32.210.959.093.4864.41.2045', ]

        query = self.session.query(HL7_Msh).filter(
            HL7_Msh.message_control_id.in_(knownIds))
        self.assertEquals(query.count(), len(knownIds))

    def testDuplicateMessageControlIds(self):
        "duplicate message control ids should only be imported once"
        # test set has these duplicates
        dup_ids = ('882379.1.564582.7826.8.77.626.',
                   '4.67200.60.28351.67009.683.7.4',
                   '794.8678.94.3702.1785.63.6925.')

        for id in dup_ids:
            query = self.session.query(HL7_Msh).filter(
                HL7_Msh.message_control_id==id)
            self.assertEquals(1, query.count())

    def testMessageType(self):
        "All message types should be saved in the database"
        types = ["ADT^A03^ADT_A03",
                 "ADT^A04^ADT_A01",
                 "ADT^A08^ADT_A01", ]

        for type in types:
            query = self.session.query(HL7_Msh)
            query.filter(HL7_Msh.message_type==type)
            self.assert_(query.count() > 0,
                         "HL7_MSH.message_type missing %s" % type)

    def testFacility(self):
        "Every row should have a facility, with a facility_lookup value"
        query = self.session.query(HL7_Msh).filter(HL7_Msh.facility is None)
        self.assert_(query.count() == 0,
                     'HL7_MSH.facility was blank for at least id %s'
                     % getattr(query.first(), 'hl7_msh_id', False))

    def testMessageDatetimeNotNull(self):
        "All rows expected to have date time"
        query = self.session.query(HL7_Msh).\
            filter(HL7_Msh.message_datetime is None)
        self.assert_(query.count() == 0, 'HL7_MSH.message_datetime '
                     'was blank for id %s' % getattr(query.first(),
                                                     'hl7_msh_id', ''))


class TestMU(SQATest):

    def test_mu_data(self):
        # Test a number of values from a MU test file 'Bfbjpo'
        id = '2.6.21919.99289.858698.379.23.'
        query = self.session.query(HL7_Msh).\
            filter(HL7_Msh.message_control_id==id)
        msh_row = query.one()
        self.assertEquals('3768573961', msh_row.facility)

        # Confirm all visit row data matchs the source
        query = self.session.query(HL7_Visit).\
            filter(HL7_Visit.hl7_msh_id==msh_row.hl7_msh_id)
        visit_row = query.one()
        self.assertEquals('761339^^^&3768573961&NPI',
                          visit_row.patient_id)
        self.assertEquals('358798^^^&3768573961&NPI',
                          visit_row.visit_id)
        self.assertEquals('99304', visit_row.zip)
        self.assertEquals(datetime.strptime('32460528115833',
                                            '%Y%m%d%H%M%S'),
                          visit_row.admit_datetime)
        self.assertEquals('M', visit_row.gender)
        self.assertEquals('Seizure', visit_row.chief_complaint)
        self.assertEquals('I', visit_row.patient_class)
        self.assertEquals('06', visit_row.disposition)
        self.assertEquals('White', visit_row.race)
        self.assertEquals('071', visit_row.county)
        self.assertEquals('9', visit_row.admission_source)
        self.assertEquals(datetime.strptime('32460530120533',
                                            '%Y%m%d%H%M%S'),
                          visit_row.discharge_datetime)

    def testMessageDatetime(self):
        "Confirm a known date from test file 'Rrliqv' is present"
        # 32421213093537 -> 3242/12/13 09:35:37
        lookfor = datetime(3242,12,13,9,35,37)
        query = self.session.query(HL7_Msh).filter(
            HL7_Msh.message_datetime == lookfor)
        self.assert_(query.count() > 0, 'HL7_MSH.message_datetime '
                     '%s was missing' % lookfor)

    def testBatchFilename(self):
        "Should find at least one entry for every file in the set."
        mi = MirthInteraction()
        for f in mi.filenames:
            file = os.path.basename(f)
            query = self.session.query(HL7_Msh).filter(
                HL7_Msh.batch_filename == file)
            self.assert_(query.count() > 0, 'HL7_MSH.batch_filename '
                         '%s was missing' % file)


class TestHl7Visit(SQATest):

    def testMshId(self):
        "The hl7_msh_id foreign key should be present"
        query = self.session.query(HL7_Visit)
        for visit in query.all():
            self.assertTrue(visit.hl7_msh_id > 0,
                            "hl7_msh_id foreign key not set in HL7_VISIT")

    def testVisitId(self):
        "Format should include a visit_id and the assigning authority"
        query = self.session.query(HL7_Visit)

        # Format should include a visit id and the assigning authority
        known_authorities = ['&3768573961&NPI',
                             '&650903.98473.0179.6039.1.333.1&ISO',
                             '&7281.82.5411.3.2.32886.2.7795.&ISO']
        pat = re.compile("(\d+)\^\^\^(.*)")
        self.assert_(query.count() > 0, "HL7_VISIT has no data")
        for visit in query.all():
            match = re.search(pat, visit.visit_id)
            self.assert_(match.group(1) > 0,
                         'visit_id contains bogus visit id: %s' %
                         visit.visit_id )
            self.assert_(match.group(2) in known_authorities,
                         'visit_id missing expected authority: %s' %
                         visit.visit_id )
            
    def testPatientId(self):
        "Format should include a patient_id and the assigning authority"
        query = self.session.query(HL7_Visit)

        # Format should include a visit id and the assigning authority
        known_authorities = ['&3768573961&NPI',
                             '&650903.98473.0179.6039.1.333.1&ISO',
                             '&7281.82.5411.3.2.32886.2.7795.&ISO']
        pat = re.compile("(\d+)\^\^\^(.*)")
        self.assert_(query.count() > 0, "HL7_VISIT has no data")
        for visit in query.all():
            match = re.search(pat, visit.patient_id)
            self.assert_(match.group(1) > 0,
                         'patient_id contains bogus visit id: %s' %
                         visit.patient_id )
            self.assert_(match.group(2) in known_authorities,
                         'patient_id missing expected authority: %s' %
                         visit.patient_id )

    def testZip(self):
        "Zips should be 3 or 5 digits"
        query = self.session.query(HL7_Visit).filter(HL7_Visit.zip <> None)

        # Expect every zip to be 3 OR 5 digits for US zipcodes.
        pat = re.compile("^\d*$")

        for visit in query.all():
            if visit.country == 'CAN':
                continue # i.e. T2T1E41 ...
            self.assert_(len(visit.zip) == 3 or \
                             len(visit.zip) == 5,
                         'zip is the wrong length, '
                         'should be 3 or 5 digits: %s %s' %
                         (visit.zip,visit.country))
            self.assert_(re.search(pat, visit.zip),
                         'zip %s contains non integers' % visit.zip)

    def testAdmitDatetime(self):
        "Confirm a few random known dates from test file 'Rrliqv' are present"
        dates = [datetime(3242,12,03,01,33,42),
                 datetime(3242,12,13,9,33,36), 
                 datetime(3242,8,23,5,34,42)]
        for d in dates:
            query = self.session.query(HL7_Visit).filter(HL7_Visit.admit_datetime == d)
            self.assert_(query.count() >= 1, 
                     "admit_datetime missing '%s'" % d)

    def testDischargeDatetime(self):
        "Confirm a few random known dates from test file 'Rrliqv' are present"
        dates = (datetime(3242,12,01,07,33,35),
                 datetime(3242,12,06,06,30,42),
                 datetime(3242,07,15,12,31,33),)
        for d in dates:
            query = self.session.query(HL7_Visit).filter(
                HL7_Visit.discharge_datetime == d)
            self.assert_(query.count() >= 1, 
                     "discharge_datetime missing '%s'" % d)

    def testGender(self):
        "Valid genders are M, F, U, and O"
        query = self.session.query(HL7_Visit).filter(HL7_Visit.gender <> None)
        valid = ['M','F','U','O']
        self.assert_(query.count() > 0, "HL7_VISIT.gender in error")
        for visit in query.all():
            self.assert_(visit.gender in valid,
                         'gender %s not valid' % visit.gender)

    def testDOB(self):
        "Dates of birth should be in the YYYYMM format"
        query = self.session.query(HL7_Visit).filter(HL7_Visit.dob <> None)
        self.assert_(query.count() > 0, "HL7_VISIT.gender in error")
        pat = re.compile("^\d{6}$")
        for visit in query.all():
            match = re.search(pat, visit.dob)
            self.assert_(match,
                         "dob %s doesn't match YYYYMM format" % visit.dob)

    def testChiefComplaint(self):
        "A few random expected values from test file 'Rrliqv'"
        complaints = ["WEAKNESS,PAIN",
                      "PYLORIC STENOSIS",
                      "DIFFICULTY BREATHING",
                      "LIP LACERATION",
                      "COLONOSCOPY DIAGNOSTIC",
                      "RECTAL BLEED WITH OVER COAGULATION",]
        for c in complaints:
            query = self.session.query(HL7_Visit).filter(
                HL7_Visit.chief_complaint == c)
            self.assert_(query.count() > 0, 
                         "chief_complaint missing %s" % c)

    def testPatientClass(self):
        "Valid patient classes are E, I, O and U"
        valid = ['E','I','O','U']
        query = self.session.query(HL7_Visit).filter(
            HL7_Visit.patient_class <> None)
        self.assert_(query.count() > 0, "HL7_VISIT.patient_class in error")

        for visit in query:
            self.assert_(visit.patient_class in valid, "patient_class "
                         "'%s' not valid" % visit.patient_class)

    def testDisposition(self):
        """Check for "standardized disposition code" - a 2 digit number"""
        sel = select((HL7_Visit.disposition,)).distinct()
        query = self.session.execute(sel)

        pat = re.compile("^\d\d$")
        for disposition in query.fetchall():
            if not disposition[0]: continue
            self.assert_(re.match(pat, disposition[0]), 
                         "disposition '%s'doesn't look valid" % disposition[0])

    def testChiefComplaintWithQuotes(self):
        "Bug fix from a cheif complaint with multiple quotes"
        complaint = "COUGH,'VERY SICK'"
        query = self.session.query(HL7_Visit).filter(HL7_Visit.chief_complaint == complaint)
        self.assert_(query.count() > 0, 
                         "chief_complaint missing %s" % complaint)

    def testCounty(self):
        "County field should be persisted"
        counties_in_test_set = ('ADA-WA','ASO-WA','BEN-ID','BEN-WA',
                                'BONN-ID','COW-WA','FER-WA','FRA-WA',
                                'GRA-WA','KIN-WA','KOO-ID','LAT-ID',
                                'LIN-MT','LIN-WA','MIS-MT','NEZPE-ID',
                                'OKA-WA','PENOR-WA','SHO-ID','SPO-WA',
                                'STE-WA','UMA-OR','WALWA-WA','WHI-WA',
                                'YAK-WA',)
        query = self.session.query(HL7_Visit.county).distinct()
        counties_in_results = [row[0] for row in query]
        self.assertTrue(len(counties_in_results) >=
                        len(counties_in_test_set))
        for c in counties_in_test_set:
            if c not in counties_in_results:
                self.fail('%s not found in db' %c)

    def testRaceEthnicity(self):
        "Race field should be persisted"
        race_eth_in_test_set = ('American Indian or Alaska Native',
                                'Asian',
                                'Black or African American',
                                'Native Hawaiian or Other Pacific Islander',
                                'White',
                                'Other Race',
                                'Hispanic or Latino',)
        query = self.session.query(HL7_Visit.race).distinct()
        race_in_results = [row[0] for row in query]
        self.assertTrue(len(race_in_results) >=
                        len(race_eth_in_test_set))
        for r in race_eth_in_test_set:
            if r not in race_in_results:
                self.fail('%s not found in db' %r)

    def testServiceCode(self):
        "Service codes"
        codes_in_test_set = ('CAN', 'CAR', 'INT', 'NBI', 'OBG', 'OBS',
                             'OTH', 'PED', 'PHY', 'PIN', 'SUR',)
        query = self.session.query(HL7_Visit.service_code).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(codes_in_test_set))
        for s in codes_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testServiceAltId(self):
        "Service Alternate Identifier"
        codes_in_test_set = ('CCU', 'GYNE', 'ICU', 'MEDI', 'MEDN',
                             'MEDO', 'MEDR', 'NBIC', 'NBTP', 'NES',
                             'NEU', 'NURS', 'OBS', 'OBV', 'OHS',
                             'OPRA', 'OPRC', 'ORTH', 'OTRT', 'PEDI',
                             'PEDO', 'PIC', 'PLSU', 'SURG', 'UROG',)
        query = self.session.query(HL7_Visit.service_alt_id).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(codes_in_test_set))
        for s in codes_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testAdmissionSource(self):
        "Admission Source"
        codes_in_test_set = ('1','2','4','5','6','7','8','9','D')
        query = self.session.query(HL7_Visit.admission_source).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(codes_in_test_set))
        for s in codes_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)
        
    def testAssignedLocation(self):
        "Assigned Patient Location"
        locations_in_test_set = (
            '2N', '2S', '4N', '4S', '5N', '5S', '6N', '6S', '7N',
            '7S', '8N', '8NURS', '8S', 'ACCS', 'ACUI', 'AN', 'BLU', 'BP',
            'BPNB', 'CARA', 'CARO', 'CON', 'D10TW', 'D11TW', 'DACU7',
            'DBS', 'DCCU', 'DCSSU', 'DCT', 'DDSC', 'DEHC', 'DER', 'DEX',
            'DHMRI', 'DMAM', 'DMLHEC', 'DMLL', 'DMLMB', 'DMLS', 'DOBD',
            'DOBM', 'DPUSD', 'DRWCLAB', 'DSSU', 'DSURG', 'DULT', 'EC',
            'EDX', 'EEG', 'EMR', 'EN', 'END', 'ENDO', 'ER', 'ER2', 'ERM',
            'ES', 'FM', 'FMCI', 'FST', 'FST2', 'GRN', 'HLBW', 'HRAG',
            'ICU', 'KPTV', 'KTP', 'LAB', 'LABSJH', 'LBW', 'LD', 'LSDO',
            'MAM', 'MEDIW', 'MRI', 'NBN', 'NICU', 'NSYI', 'NUR', 'OB',
            'OBG', 'ONM', 'OPP', 'OPRA', 'OPSD', 'P9N', 'PDIM', 'PDIS',
            'PDON', 'PEDO', 'PEDS', 'PFC', 'PICU', 'PM', 'PMAC', 'PMC',
            'PMCLAB', 'PMCURC', 'POLAB', 'PRP', 'RAC', 'RAD', 'RAG',
            'RAM', 'RAS', 'RAU', 'RED', 'RHC', 'ROOV', 'RSP', 'SCSG',
            'SDC', 'SDS', 'SERI', 'SHL', 'SMAU', 'SPC', 'SSS', 'SSU',
            'SURI', 'TED', 'TED-MC', 'TEMP', 'TXR', 'US', 'VER',
            'VMEDSURG', 'VSO', 'WER', 'XRA', 'YELO')
        query = self.session.query(HL7_Visit.\
                                   assigned_patient_location).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(locations_in_test_set))
        for s in locations_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testState(self):
        "State"
        states_in_test_set = ('AZ','CN','ID','LA','MT','OR','TX',
                              'WA',)
        query = self.session.query(HL7_Visit.state).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(states_in_test_set))
        for s in states_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)


class TestHl7Dx(SQATest):

    def testDxRank(self):
        "Confirm rank is sticking on all dx messages"
        query = self.session.query(HL7_Dx)
        for dx in query:
            self.assert_(dx.rank > 0)  # the default

    def testDxCode(self):
        "Look up a few known dx_codes from test set"
        codes = ["787.01","847.0","850.9","920","922.31","923.00","996.73",
                 "E879.1","E885.9","E928.9","E947.8","E968.8","V09.0","V12.04",
                 "V22.1","V27.0",]
        for c in codes:
            query = self.session.query(HL7_Dx).filter(HL7_Dx.dx_code == c)
            self.assert_(query.count() >= 1, "dx_code missing value '%s'" % c)

    def testDxDescription(self):
        "Look up a few known dx_descriptions from test set"
        descripts = ["OTH LYMPHOMAS EXTRANODAL   SOLID ORGAN UNSPECIFIED",
                     "DIAB MELL WO COMPL, TYPE II OR UNSPEC TYPE, NOT UN",
                     "DIAB W RENAL MANIFEST, TYPE II OR UNSPEC TYPE, NOT",
                     "HYPERLIPIDEMIA NEC/NOS",
                     "OBESITY, NOS",
                     "PANCYTOPENIA",
                     "ANEMIA NEOPLASTIC DIS",
                     "THROMBOCYTOPENIA NOS",
                     "SCHIZOPHRENIA NOS-UNSPEC",
                     "HYPTNSV CHR KID DIS, UNSPEC, W CHR KD STAGE V OR E",
                     "BRONCHITIS NOS",
                     "ASTHMA, UNSPECIFIED",
                     "UTERINE TUMOR-DELIVERED",
                     "PREV CESAREAN DELIVRY W/ OR W/O MENT ANTEPART COND",
                     "PREV CESAREAN DELIVERY, ANTEPARTUM COND OR COMPLIC",
                     "JOINT PAIN-SHLDER",
                     "CLEFT PALATE   LIP NOS",
                     "OTHER PRETERM INFANTS, 2000-2500 GRAMS",
                     "FETAL/NEONATAL JAUND NOS",
                     "DIZZINESS AND GIDDINESS",
                     "CONCUSSION NOS",
                     "CONTUSION FACE/SCALP/NCK",]

        for d in descripts:
            query = self.session.query(HL7_Dx).filter(HL7_Dx.dx_description == d)
            self.assert_(query.count() >= 1, "dx_code missing value '%s'" % d)

    def testDxType(self):
        """Only three expected types:
          A^Admitting^HL70052^A^^L
          F^Final^HL70052^F^^L
          W^Working^HL70052^W^^L"""
        expected = ['A','F','W']
        query = self.session.query(HL7_Dx).filter(HL7_Dx.dx_type <> None)
        self.assert_(query.count() > 0, "dx_type lacks data")
        for dx in query.all():
            self.assert_(dx.dx_type in expected,
                         'dx_type %s is not valid' % dx.dx_type)

    def testBackslachInDescription(self):
        "Handle backslashes found in the diagnosis description"
        # This visit should have 4 diagnoses including one with
        # a couple backslashes in the description
        query = self.session.query(HL7_Dx).join(
            (HL7_Visit, HL7_Dx.hl7_msh_id==HL7_Visit.hl7_msh_id)).\
            filter(HL7_Visit.visit_id==\
                       '093601^^^&650903.98473.0179.6039.1.333.1&ISO')
        self.assertEquals(query.count(), 4)
        found = False
        for dx in query:
            if dx.dx_code == '793.99':
                self.assertEquals(
                    dx.dx_description,
                    "OTH NOSP (ABN) FINDINGS RADIOLOGICAL \T\\ ")
                found = True
        self.assert_(found, "Missing dx with backslash")


class TestHl7Obr(SQATest):

    def testLoincCode(self):
        "Look up a few known LOINC codes from test set"
        codes = ("600-7", "45187-2")

        for c in codes:
            query = self.session.query(HL7_Obr).filter(
                HL7_Obr.loinc_code == c)
            self.assert_(query.count >= 1, "loinc_code '%s' missing" % c)

    def testLoincText(self):
        "Look up a few known loinc text fields from test set"
        expected = ("Bacteria identified:Prid:Pt:Bld:Nom:Culture",
                 "Antibiotic XXX:Susc:Pt:Isolate:OrdQn:Agar diffusion")

        for e in expected:
            query = self.session.query(HL7_Obr).filter(
                HL7_Obr.loinc_text == e)
            self.assert_(query.count() >= 1, "loinc_text '%s' missing" % e)

    def testAltText(self):
        "Check for expected alt_text"
        expected = ("Culture Blood","KB Susceptibility")
        for e in expected:
            query = self.session.query(HL7_Obr).filter(
                HL7_Obr.alt_text == e)
            self.assert_(query.count() >= 1, "alt_text '%s' missing" 
                         % e)
    def testStatus(self):
        stati = ('A','F','P')
        query = self.session.query(HL7_Obr.status).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(stati))
        for s in stati:
            if s not in results:
                self.fail('%s not found in db' %s)
        
    def testReportDatetime(self):
        "Report datetime"
        a_few_from_test_set = (
            datetime(3243, 4,13, 3,30,33),
            datetime(3243, 4,13,15,34,38),
            datetime(3243, 4,18, 9,30,33),
            datetime(3244, 7, 4,22,33,33),
            datetime(3244, 7, 5, 2,34,40),
            datetime(3244, 7, 5, 6,30,33),
            datetime(3244, 7, 5, 6,34,39),
            datetime(3244, 7, 5, 6,34,40),
            datetime(3244, 7, 5, 6,34,41),
            datetime(3346,12, 1,22,30,36),
            )
        query = self.session.query(HL7_Obr.report_datetime).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(a_few_from_test_set))
        for s in a_few_from_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)
        
    def testSpecimenSource(self):
        "Specimen Source"
        a_few_from_test_set = (
            'ABD', 'ANTRUM', 'BAL', 'BLUD', 'BRUESO', 'CERVIX',
            'CSF', 'DWA', 'DWB', 'FOOT', 'LEG', 'LYM', 'NASAL', 'NP',
            'PERITO', 'PF', 'STOOL', 'SWO', 'TF', 'THOFLD', 'THROAT',
            'THT', 'UCV', 'UFC', 'UMC', 'UO', 'URICC', 'URINE', 'URISCA',
            'VAG', 'VAGINA', 'VAGREC', 'WASHBR', 
            )
        query = self.session.query(HL7_Obr.specimen_source).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(a_few_from_test_set))
        for s in a_few_from_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testFillerOrder(self):
        "Filler Order Number"
        a_few_from_test_set = (
            '4.8294.11.7.2601.3.1.58.2.1272',
            '7099007569',
            '454368.9518.831021.0.522.06960',
            '6375556977',
            '9937702778',
            '5548806817',
            '4.41.7.0.922032.304.52.018768.',
            )
        query = self.session.query(HL7_Obr.filler_order_no).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(a_few_from_test_set))
        for s in a_few_from_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testCoding(self):
        "Coding"
        query = self.session.query(HL7_Obr.coding).filter(
            HL7_Obr.coding=='LN')
        self.assertTrue(query.count() >= 1300)

    def testAltCode(self):
        "alt code"
        a_few_from_test_set = (
            '12HIVR',
            'AASGS',
            'ABO',
            'BC',
            'BS',
            'HCVRNAQ',
            'HEPA',
            'HEP-C',
            'HIV1RUQ',
            'HPS',
            'IC',
            )
        query = self.session.query(HL7_Obr.alt_code).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(a_few_from_test_set))
        for s in a_few_from_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)

    def testAltCoding(self):
        "Alt Coding"
        query = self.session.query(HL7_Obr.alt_coding).filter(
            HL7_Obr.alt_coding=='L')
        self.assertTrue(query.count() >= 158)


class TestHl7Obx(SQATest):

    def testValue(self):
        "Check for expected value_types"
        expected = ("CE", "TX")

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.value_type == e)
            self.assert_(query.count() >= 1, "value_type '%s' missing" 
                         % e)

    def testObservationValue(self):
        "Check for expected observation values"
        observations = [
            'Microorganism or agent identified:Prid:Pt:XXX:Nom:',
            'Amoxicillin+Clavulanate:Susc:Pt:Isolate:OrdQn:Agar diffusion',
            'Cefazolin:Susc:Pt:Isolate:OrdQn:Agar diffusion',
            'Ceftazidime:Susc:Pt:Isolate:OrdQn:Agar diffusion']

        for ob in observations:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.observation_text == ob)
            self.assert_(query.count() > 0,
                         'observation_text was missing: %s' % ob)

    def testObxWithoutObr(self):
        "Confirm we're saving obx rows w/o a preceeding obr"

        # a couple known values from test set that don't
        # show up when only linking w/ hl7_obr
        observation_ids = ['43137-9','29553-5'] 

        for id in observation_ids:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.observation_id == id)
            self.assert_(query.count() > 0,
                         'observation_id (one w/o an obr link) '
                         'was missing: %s' % id)

    def testObservationDateTime(self):
        "Confirm we're saving the datetime of the observation"
        
        # To date, the only OBX rows w/ date time values are from
        # facility info.  OBR however frequently shows dates.  Make
        # sure we are correctly parsing those (using hand scraped
        # values from test file Oshroj

        """select observation_datetime from hl7_obr join hl7_msh on
        hl7_msh.hl7_msh_id = hl7_obr.hl7_msh_id where
        hl7_msh.batch_filename = 'Oshroj'"""
        expected_datetimes = (
            '3243-04-18 09:34:38',
            '3243-04-18 09:34:38',
            '3243-04-17 18:30:34',
            '3243-04-18 09:30:33',
            '3243-04-17 18:30:34',
            '3243-04-18 08:30:33',
            '3243-04-18 08:30:33',
            '3243-04-18 08:30:33',
            '3243-04-18 08:30:33',
            '3243-04-18 08:30:33',
            '3243-04-18 08:30:33',
            '3243-04-18 09:35:38',
            '3243-04-18 10:31:38',
            '3243-04-18 09:35:39',
            )
        for datetime in expected_datetimes:
            query = \
                self.session.query(HL7_Obr).filter(HL7_Obr.observation_datetime
                                               == datetime)
            self.assert_(query.count() > 0,
                         'observation_datetime was missing: %s' %
                         datetime)
            
    def testGeneralOrder(self):
        "Look for OBR data from ORM^O01 (general order messages)"
        expected_loinc_codes = ('630-4', '664-3', '664-3', '664-3',
                                '580-1', '543-9', '593-4', '588-4',
                                '700-5', '600-7', '600-7', '34468-9')

        for loinc_code in expected_loinc_codes:
            query = self.session.query(HL7_Obr).filter(
                HL7_Obr.loinc_code == loinc_code)
            self.assert_(query.count() > 0,
                         'loinc_code (from ORM^O01) was missing: %s' %
                         loinc_code)

    def testPerformingLab(self):
        "performing_lab"
        for lab in ('Ttvzevrbky', 'Majpsarsbg'):
            query = self.session.query(HL7_Obx).\
                    filter(HL7_Obx.performing_lab_code == lab)
            self.assertTrue(query.count() > 0)

    def testSequence(self):
        "obx.sequence (sub-id)"
        # Currently one test matches this, returning 26 related
        # hl7_obx statements - good one for sequence check:
        dt = datetime.strptime('3244-07-05 06:33:42',
                               "%Y-%m-%d %H:%M:%S")
        query = self.session.query(HL7_Obx).\
                join((HL7_Obr,
                      HL7_Obr.hl7_obr_id==HL7_Obx.hl7_obr_id)).\
                      filter(and_(HL7_Obr.loinc_code==\
                             '21020-3', HL7_Obr.report_datetime==dt)).\
                             order_by(HL7_Obx.hl7_obx_id)
        self.assertEquals(query.count(), 19)
        last_sd, last_pt = 0,0
        for e in query:
            sd, pt = e.sequence.split('.')
            if int(pt) < last_pt:
                self.assertTrue(int(sd) > last_sd)
            last_sd = int(sd)
            last_pt = int(pt)

    def testUnits(self):
        "obx.units"
        # All these unique unit values were found by hand parsing
        # testfile Wpqvqn - confirm their presence
        expected = ('Years', 'fl', 'g/dl', 'IU/mL', 'K/CMM', '%',
                    'M/CMM', 'pg')
        query = self.session.query(HL7_Obx.units).distinct()
        found = [e[0] for e in query]
        for unit in expected:
            self.assertTrue(unit in found)

    def test_coding(self):
        "OBX coding"
        expected = ("LN", "NULLFL")

        for e in expected:
            query = self.session.query(HL7_Obx).filter(HL7_Obx.coding == e)
            self.assert_(query.count() >= 1, "coding '%s' missing" 
                         % e)

    def test_alt_id(self):
        "OBX alt_id"
        expected = ('TZP', 'URI*', 'VAN', 'VZS*', 'WBC', 'WBC*', 'WC*',)
        for e in expected:
            query = self.session.query(HL7_Obx).filter(HL7_Obx.alt_id == e)
            self.assert_(query.count() >= 1, "alt_id '%s' missing" 
                         % e)

    def test_alt_text(self):
        "OBX alt_text"
        expected = (
            "RBC DISTRIBUTION WIDTH-SD",
            "REPORT STATUS",
            "RESPIRATORY VIRAL SCREEN",
            "Result",
            "RH(D)",
            "RPR",
            "RUBELLA AB, IGG",
            "SODIUM",
            "SPECIMEN SOURCE",
            "Tetracycline",
            "TETRACYCLINE",
            )            

        for e in expected:
            query = self.session.query(HL7_Obx).filter(HL7_Obx.alt_text == e)
            self.assert_(query.count() >= 1, "alt_text '%s' missing" 
                         % e)

    def test_alt_coding(self):
        "OBX alt_coding"
        expected = ("L",)

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.alt_coding == e)
            self.assert_(query.count() >= 1, "alt_coding '%s' missing" 
                         % e)

    def test_reference_range(self):
        "OBX reference_range"
        expected = (
            "0.6-1.4",
            "0.9-2.8",
            "10-20",
            "1.0-4.8",
            "11.0-14.5",
            "12.0-16.0",
            "12-45",
            "<129",
            "8-35",
            "8.4-10.2",
            ">9",
            "9.4-12.5",
            "98-108",
            "NEGATIVE",
            "NR",
            )            

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.reference_range == e)
            self.assert_(query.count() >= 1, "reference_range '%s' missing" 
                         % e)

    def test_abnorm_id(self):
        "OBX abnorm_id"
        expected = ('H', 'L', 'R', 'S')

        for e in expected:
            query = self.session.query(HL7_Obx).filter(HL7_Obx.abnorm_id == e)
            self.assert_(query.count() >= 1, "abnorm_id '%s' missing" 
                         % e)

    def test_abnorm_text(self):
        "OBX abnorm_text"
        expected = (
            "Above high normal",
            "Below low normal",
            "Resistant. Indicates for microbiology susceptibilities only.",
            "Susceptible. Indicates for microbiology susceptibilities only.",
            )
        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.abnorm_text == e)
            self.assert_(query.count() >= 1, "abnorm_text '%s' missing" 
                         % e)

    def test_abnorm_coding(self):
        "OBX abnorm_coding"
        expected = ("HL70078",)

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.abnorm_coding == e)
            self.assert_(query.count() >= 1, "abnorm_coding '%s' missing" 
                         % e)

    def test_alt_abnorm_id(self):
        "OBX alt_abnorm_id"
        expected = ('H', 'L', 'R', 'R*', 'S')

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.alt_abnorm_id == e)
            self.assert_(query.count() >= 1, "alt_abnorm_id '%s' missing" 
                         % e)

    def test_alt_abnorm_text(self):
        "OBX alt_abnorm_text"
        expected = ('Susceptible. Indicates for microbiology '
                    'susceptibilities only.',)

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.alt_abnorm_text == e)
            self.assert_(query.count() >= 1, "alt_abnorm_text '%s' missing" 
                         % e)

    def test_alt_abnorm_coding(self):
        "OBX alt_abnorm_coding"
        expected = ("L", "HL70078")

        for e in expected:
            query = self.session.query(HL7_Obx).filter(
                HL7_Obx.alt_abnorm_coding == e)
            self.assert_(query.count() >= 1, "alt_abnorm_coding '%s' missing" 
                         % e)


class TestHl7Spm(SQATest):

    def testSpm(self):
        "Check all values from a few known spm rows came through"
        sample = '122561005^Blood specimen from patient ' +\
                 '(specimen)^SN^BLUD^Blood^L'
        fields = sample.split('^')
        id, description, code = fields[0], fields[1], fields[3]
        
        query = self.session.query(HL7_Spm).\
                filter(HL7_Spm.id == id)
        self.assertTrue(query.count() >= 1)
        descriptions, codes = [], []
        for q in query:
            descriptions.append(q.description)
            codes.append(q.code)
        
        self.assertTrue(description in descriptions)
        self.assertTrue(code in codes)
        
    def testSpmCodes(self):
        "Confirm all codes came through"
        codes = (
            'ABD', 'BAL', 'BLUD', 'CERVIX', 'CSF', 'DWA', 'DWB',
            'FOOT', 'LEG', 'LYM', 'NASAL', 'NP', 'PERITO', 'PF',
            'STOOL', 'SWO', 'TF', 'THROAT', 'THT', 'UCV', 'UFC',
            'URICC', 'URINE', 'URISCA', 'VAGINA', 'VAGREC', 'WASHBR',
        )
        query = self.session.query(HL7_Spm.code).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(codes))
        for s in codes:
            if s not in results:
                self.fail('%s not found in db' %s)


class TestObservationData(SQATest):

    def testAssociations(self):
        "Confirm OBX and SPM data is available"
        lc = '610-6'
        v_id = '667570^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(ObservationData).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==HL7_Obr.hl7_msh_id)).\
                     filter(and_(HL7_Visit.visit_id==v_id,
                                 HL7_Obr.loinc_code==lc))
        lab_msg = query.one()
        self.assertEquals(len(lab_msg.spms), 1)
        self.assertEquals(len(lab_msg.obxes), 2)
        
    def testVisitLookup(self):
        "Confirm we can obtain all for a single visit in one query"
        v_id = '950039^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(ObservationData).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==HL7_Obr.hl7_msh_id)).\
                filter(HL7_Visit.visit_id == v_id)
        # Should see two ObservationData, both with obxes
        found = 0
        for r in query:
            self.assertTrue(len(r.obxes) > 0)
            found += 1
        self.assertEquals(found, 2)

    def dontestMegaJoin(self):
        v_id = '950039^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(ObservationData,HL7_Visit).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==HL7_Obr.hl7_msh_id),).\
                filter(HL7_Visit.visit_id == v_id)
        for r,j in query:
            print r
            print j

    def testFullMessageByVisit(self):
        "8 MSH for one visit - check a variety of info"
        v_id = '405774^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(FullMessage).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==FullMessage.hl7_msh_id),).\
                filter(HL7_Visit.visit_id == v_id).\
                order_by(FullMessage.message_datetime)
        self.assertEquals(query.count(), 8)

    def testMessageW9(self):
        nine = '467984^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(FullMessage).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==FullMessage.hl7_msh_id),).\
                filter(HL7_Visit.visit_id == nine)
        self.assertEquals(query.count(), 1)
        msg = query.one()
        self.assertEquals(len(msg.dxes), 9)

    def testIPlabStatus(self):
        "OBR 25.1 should be single char, map 'IP' to 'I'"
        visit = '472781^^^&650903.98473.0179.6039.1.333.1&ISO'
        query = self.session.query(HL7_Obr).\
                join((HL7_Visit,
                     HL7_Visit.hl7_msh_id==HL7_Obr.hl7_msh_id),).\
                filter(HL7_Visit.visit_id == visit)
        self.assertEquals(query.count(), 1)
        msg = query.one()
        self.assertEquals(msg.status, 'I')


class TestHl7Nte(SQATest):

    def testNte(self):
        "Check for existence of expected notes from test set"
        notes_in_test_set = (
            'Rcmamkdjer',
            'Oehgwlpwby',
            'Qfvahxhaiy',
            'Sotfpqpkyf',
            'Bmalwxszmf',
            'Melpbusdee',
            'Nybhpeyfgz',
            'Agrqjnpzlj',
            'Jxlnfftcvx',
            'Wjkgypfgju',
            'Sozcdaxjan',
            'Enemiynoyz',
            'Pnvzkuuwpt',
            'Nhxidnrfrg',
            'Tfolwwtfcl',
            'Meybschdzi',
            'Sixasldidd',
            )
        query = self.session.query(HL7_Nte.note).distinct()
        results = [row[0] for row in query]
        self.assertTrue(len(results) >= len(notes_in_test_set))
        for s in notes_in_test_set:
            if s not in results:
                self.fail('%s not found in db' %s)
