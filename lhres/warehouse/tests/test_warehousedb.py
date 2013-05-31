from datetime import datetime
import unittest

from lhres.util.config import Config
from lhres.util.pg_access import AlchemyAccess
from lhres.warehouse.tables import create_tables
from lhres.warehouse.tables import HL7_Dx
from lhres.warehouse.tables import HL7_Msh
from lhres.warehouse.tables import HL7_Nte
from lhres.warehouse.tables import HL7_Obr
from lhres.warehouse.tables import HL7_Obx
from lhres.warehouse.tables import HL7_RawMessage
from lhres.warehouse.tables import HL7_Spm
from lhres.warehouse.tables import HL7_Visit


def setup_module():
    """Create a fresh db (once) for all tests in this module"""
    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")

    param = lambda v: c.get('warehouse', v)
    create_tables(param('create_table_user'),
                  param('create_table_password'),
                  param('database'),
                  enable_delete=True)


class testSqlAObjects(unittest.TestCase):
    """We should be able to create and work with objects
    that are based on tables in the database
    """
    def setUp(self):
        c = Config()
        param = lambda v: c.get('warehouse', v)
        self.alchemy = AlchemyAccess(database=param('database'),
                                     host='localhost',
                                     user=param('database_user'),
                                     password=param('database_password'))
        self.session = self.alchemy.session


    def tearDown(self):
        # Purge the unittest hl7_msh and all related data
        self.session.delete(self.msh)
        self.session.commit()
        self.alchemy.disconnect()

    def testABuildTables(self):
        """We need to build dependent tables in the correct order.
        """
        self.tHL7_Msh()
        self.tHL7_RawMessage()
        self.tHL7_Visit()
        self.tHL7_Dx()
        self.tHL7_Obr()
        self.tHL7_Obx()

    def tHL7_RawMessage(self):
        """Create an HL7_RawMessage object that is saved to the database"""
        mess = HL7_RawMessage(hl7_raw_message_id=1,
                           message_control_id=u'control_id',
                           raw_data=u'some raw data')
        #Add the new message to the session
        self.session.add(mess)
        self.session.commit()

        query = self.session.query(HL7_RawMessage).\
                filter(HL7_RawMessage.hl7_raw_message_id == 1)

        self.assert_(query.count() == 1,
                     'The message we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_RawMessage 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_RawMessage 1>\nGot: %s' % result)

        #Make sure all the fields came out as expected
        self.assert_(result.hl7_raw_message_id == 1,
                     'hl7_raw_message_id invalid.\nExpected: '\
                     '1\nGot: %s' % result.hl7_raw_message_id)
        self.assert_(result.message_control_id == 'control_id',
                     'message_control_id invalid.\nExpected: '\
                     'control_id\nGot: %s' % result.message_control_id)
        self.assert_(result.raw_data == 'some raw data',
                     'raw_data invalid.\nExpected: some raw '\
                     'data\nGot: %s' % result.raw_data)

    def tHL7_Msh(self):
        """Create an HL7_Msh object that is saved to the database"""
        self.msh = HL7_Msh(hl7_msh_id=1,
                           message_control_id=u'control_id',
                           message_type=u'message type',
                           facility=u'facility',
                           message_datetime=datetime(2007, 01, 01),
                           batch_filename=u'183749382629734')

        #Add the new msh to the session
        self.session.add(self.msh)
        self.session.commit()
        query = self.session.query(HL7_Msh)
        self.assert_(query.count() == 1,
                     'The msh we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_Msh 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_RawMessage 1>\nGot: %s' % result)

        #Make sure all the fields came out as expected
        self.assert_(result.hl7_msh_id == 1,
                     'hl7_msh_id invalid.\nExpected: 1\nGot: '\
                     '%s' % result.hl7_msh_id)
        self.assert_(result.message_control_id == 'control_id',
                     'message_control_id invalid.\nExpected: '\
                     'control_id\nGot: %s' % result.message_control_id)
        self.assert_(result.message_type == 'message type',
                     'message_type invalid.\nExpected: message '\
                     'type\nGot: %s' % result.message_type)
        self.assert_(result.facility == 'facility',
                     'facility invalid.\nExpected: '\
                     'facility\nGot: %s' % result.facility)
        self.assert_(result.message_datetime ==
                     datetime(2007, 01, 01, 0, 0),
                     'message_datetime invalid.\nExpected: '\
                     '2007-01-01 00:00:00\nGot: %s' % result.message_datetime)
        self.assert_(result.batch_filename == '183749382629734',
                     'batch_filename invalid.\nExpected: '\
                     '183749382629734\nGot: %s' % result.batch_filename)

    def tHL7_Visit(self):
        """Create an HL7_Visit object that is saved to the database"""
        visit = HL7_Visit(hl7_visit_id=1,
                          visit_id=u'45',
                          patient_id=u'patient id',
                          zip=u'zip',
                          admit_datetime=datetime(2007, 01, 01),
                          gender=u'F',
                          dob=u'2001,01',
                          chief_complaint=u'Pain',
                          patient_class=u'1',
                          hl7_msh_id=1,
                          disposition='01',
                          state='WA',
                          admission_source='Emergency room',
                          assigned_patient_location='MVMGREF')

        #Add the new msh to the session
        self.session.add(visit)
        self.session.commit()

        query = self.session.query(HL7_Visit)
        self.assert_(query.count() == 1,
                     'The visit we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_Visit 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_Visit 1>\nGot: %s' % result)

        #Make sure all the fields came out as expected
        self.assert_(result.hl7_visit_id == 1,
                     'hl7_visit_id invalid.\nExpected: '\
                     '1\nGot: %s' % result.hl7_visit_id)
        self.assert_(result.visit_id == '45',
                     'visit_id invalid.\nExpected: 45\nGot: '\
                     '%s' % result.visit_id)
        self.assert_(result.patient_id == 'patient id',
                     'patient_id invalid.\nExpected: patient '\
                     'id\nGot: %s' % result.patient_id)
        self.assert_(result.zip == 'zip',
                     'zip invalid.\nExpected: zip\nGot: %s' % result.zip)
        self.assert_(result.admit_datetime == datetime(2007, 01, 01),
                     'admit_datetime invalid.\nExpected: '\
                     '2007-01-01 00:00:00\nGot: %s' % result.admit_datetime)
        self.assert_(result.gender == 'F',
                     'gender invalid.\nExpected: F\nGot: %s' % result.gender)
        self.assert_(result.dob == '2001,01',
                     'dob invalid.\nExpected: 2001-01-10 '\
                     '00:00:00\nGot: %s' % result.dob)
        self.assert_(result.chief_complaint == 'Pain',
                     'chief_complaint invalid.\nExpected: '\
                     'Pain\nGot: %s' % result.chief_complaint)
        self.assert_(result.patient_class == '1',
                     'patient_class invalid.\nExpected: '\
                     '1\nGot: %s' % result.patient_class)
        self.assert_(result.disposition == '01',
                     'disposition invalid.\nExpected: '\
                     '01\nGot: %s' % result.disposition)
        self.assertEquals(result.state, 'WA')
        self.assertEquals(result.admission_source, 'Emergency room')
        self.assertEquals(result.assigned_patient_location, 'MVMGREF')

    def tHL7_Dx(self):
        """Create an HL7_Dx object that is saved to the database"""
        dx = HL7_Dx(hl7_dx_id=1,
                    dx_code=u'dx code',
                    dx_description=u'description',
                    dx_type=u'A',
                    hl7_msh_id=1)

        #Add the new msh to the session
        self.session.add(dx)
        self.session.commit()

        query = self.session.query(HL7_Dx)
        self.assert_(query.count() == 1,
                     'The dx we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_Dx 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_Dx 1>\nGot: %s' % result)

        self.assert_(result.hl7_dx_id == 1,
                     'hl7_dx_id invalid.\nExpected: 1\nGot: '\
                     '%s' % result.hl7_dx_id)
        self.assert_(result.dx_code == 'dx code',
                     'dx_code invalid.\nExpected: dx code\nGot: '\
                     '%s' % result.dx_code)
        self.assert_(result.dx_description == 'description',
                     'dx_description invalid.\nExpected: '\
                     'description\nGot: %s' % result.dx_description)
        self.assert_(result.dx_type == 'A',
                     'dx_type invalid.\nExpected: A\nGot: %s' % result.dx_type)

    def tHL7_Obr(self):
        """Create an HL7_Obr object that is saved to the database"""
        dt = datetime.now()
        obr = HL7_Obr(hl7_obr_id=1,
                      loinc_code=u'loinc code',
                      loinc_text=u'loinc text',
                      alt_text=u'alt text',
                      hl7_msh_id=1,
                      status='W',
                      report_datetime=dt,
                      specimen_source='NASAL')

        #Add the new msh to the session
        self.session.add(obr)
        self.session.commit()

        query = self.session.query(HL7_Obr)
        self.assert_(query.count() == 1,
                     'The obr we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_Obr 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_Obr 1>\nGot: %s' % result)

        self.assert_(result.hl7_obr_id == 1,
                     'hl7_obr_id invalid.\nExpected: 1\nGot: '\
                     '%s' % result.hl7_obr_id)
        self.assert_(result.loinc_code == 'loinc code',
                     'loinc_code invalid.\nExpected: '\
                     'loinc code\nGot: %s' % result.loinc_code)
        self.assert_(result.loinc_text == 'loinc text',
                     'loinc_text invalid.\nExpected: '\
                     'loinc text\nGot: %s' % result.loinc_text)
        self.assert_(result.alt_text == 'alt text',
                     'alt text invalid.\nExpected: alt '\
                     'text\nGot: %s' % result.alt_text)
        self.assertEquals(result.status, 'W')
        self.assertEquals(result.report_datetime, dt)
        self.assertEquals(result.specimen_source, 'NASAL')

    def tHL7_Obx(self):
        """Create an HL7_Obx object that is saved to the database"""
        obx = HL7_Obx(hl7_obx_id=1,
                      hl7_obr_id=1,
                      value_type='vt',
                      observation_id=u'observation id',
                      observation_text=u'observation text',
                      observation_result=u'observation result',
                      units=u'units',
                      result_status=u'result status',
                      observation_datetime=datetime(2001, 1, 1),
                      hl7_msh_id=1,
                      performing_lab_code='SHMC')
        #Add the new msh to the session
        self.session.add(obx)
        self.session.commit()

        query = self.session.query(HL7_Obx)
        self.assert_(query.count() == 1,
                     'The obx we created was not found')

        result = query.first()
        #Check that the __repr__ is working as expected
        self.assert_(result.__repr__() == '<HL7_Obx 1>',
                     'Message string invalid.\nExpected: '\
                     '<HL7_Obx 1>\nGot: %s' % result)

        self.assert_(result.hl7_obx_id == 1,
                     'hl7_obx_id invalid.\nExpected: '\
                     '1\nGot: %s' % result.hl7_obx_id)
        self.assert_(result.hl7_obr_id == 1,
                     'hl7_obr_id invalid.\nExpected: '\
                     '1\nGot: %s' % result.hl7_obr_id)
        self.assert_(result.value_type.strip() == 'vt',
                     'value_type invalid.\nExpected: '\
                     'vt\nGot: %s' % result.value_type)
        self.assert_(result.observation_text == 'observation text',
                     'observation_text invalid.\nExpected: '\
                     'observation text\nGot: %s' % result.observation_text)
        self.assert_(result.observation_result == 'observation result',
                     'observation_result invalid.\nExpected: '\
                     'observation result\nGot: %s' % result.observation_result)
        self.assert_(result.units == 'units',
                     'units invalid.\nExpected: units\nGot: %s'
                     % result.units)
        self.assert_(result.result_status == 'result status',
                     'result_status invalid.\nExpected: result '\
                     'status\nGot: %s' % result.result_status)
        self.assert_(result.observation_datetime == datetime(2001, 1, 1),
                     'observation_datetime invalid.\nExpected: '\
                     '2001-01-01 00:00:00\nGot: %s' %
                     result.observation_datetime)
        self.assertEquals(result.performing_lab_code, 'SHMC')

    def testObxRelation(self):
        "Use sqlalchemy relations for automated obx/obr relations "
        # Need an HL7_Msh for foreign key constraint conformance
        self.msh = HL7_Msh(hl7_msh_id=1,
                           message_control_id=u'control_id',
                           message_type=u'message type',
                           facility=u'facility',
                           message_datetime=datetime(2007, 01, 01),
                           batch_filename=u'183749382629734')

        obr = HL7_Obr(loinc_code=u'loinc code',
                      loinc_text=u'loinc text',
                      alt_text=u'alt text',
                      hl7_msh_id=self.msh.hl7_msh_id)

        obx = HL7_Obx(value_type='vt',
                      observation_id=u'observation id',
                      observation_text=u'observation text',
                      observation_result=u'observation result',
                      units=u'units',
                      result_status=u'result status',
                      observation_datetime=datetime(2001, 1, 1),
                      hl7_msh_id=self.msh.hl7_msh_id)
        obr.obxes.append(obx)
        self.session.add(self.msh)
        self.session.commit()
        self.session.add(obr)
        self.session.commit()

        # See if the commit cascaded.  If so, the obx will have a
        # valid pk and the obr foreign key set.
        self.assertEquals(obr.hl7_obr_id, obx.hl7_obr_id)

        # Now query for the obr, see if the obx is in tow.
        roundTripObr = self.session.query(HL7_Obr).one()
        self.assertTrue(roundTripObr.hl7_obr_id > 0)
        self.assertEquals(type(roundTripObr.obxes[0]), type(obx))
        self.assertEquals(roundTripObr.obxes[0], obx)

    def testNte(self):
        """Test HL7_Nte table access """
        self.msh = HL7_Msh(hl7_msh_id=1,
                      message_control_id=u'control_id',
                      message_type=u'message type',
                      facility=u'facility',
                      message_datetime=datetime(2007, 01, 01),
                      batch_filename=u'183749382629734')
        self.session.add(self.msh)
        self.session.commit()

        obr = HL7_Obr(hl7_obr_id=1,
                      loinc_code=u'loinc code',
                      loinc_text=u'loinc text',
                      alt_text=u'alt text',
                      hl7_msh_id=1,
                      status='W',
                      report_datetime=datetime.now(),
                      specimen_source='NASAL')
        self.session.add(obr)
        self.session.commit()

        obx = HL7_Obx(hl7_obx_id=1,
                      hl7_obr_id=1,
                      value_type='vt',
                      observation_id=u'observation id',
                      observation_text=u'observation text',
                      observation_result=u'observation result',
                      units=u'units',
                      result_status=u'result status',
                      observation_datetime=datetime(2001, 1, 1),
                      hl7_msh_id=1,
                      performing_lab_code=u'SHMC',
                      sequence=u'1.1',)
        self.session.add(obx)
        self.session.commit()

        note = HL7_Nte(sequence_number=1,
                       note='fascinating unittest note',
                       hl7_obx_id=1)
        self.session.add(note)
        self.session.commit()
        query = self.session.query(HL7_Nte)
        self.assertEquals(query.count(), 1)
        self.assertEquals(query.one().note,
                          'fascinating unittest note')
        self.assertEquals(query.one().sequence_number, 1)

    def testSpecimenSource(self):
        """Test HL7_Spm table access """
        self.msh = HL7_Msh(hl7_msh_id=1,
                      message_control_id=u'control_id',
                      message_type=u'message type',
                      facility=u'facility',
                      message_datetime=datetime(2007, 01, 01),
                      batch_filename=u'183749382629734')
        self.session.add(self.msh)
        self.session.commit()

        obr = HL7_Obr(hl7_obr_id=1,
                      loinc_code=u'loinc code',
                      loinc_text=u'loinc text',
                      alt_text=u'alt text',
                      hl7_msh_id=1,
                      status='W',
                      report_datetime=datetime.now(),
                      specimen_source='NASAL')
        self.session.add(obr)
        self.session.commit()

        spm = HL7_Spm(id='123', description="your belly",
                      code='bly', hl7_obr_id=1)
        self.session.add(spm)
        self.session.commit()
        query = self.session.query(HL7_Spm)
        self.assertEquals(query.count(), 1)
        self.assertEquals(query.one().description, 'your belly')
        self.assertEquals(query.one().code, 'bly')


if '__main__' == __name__:  # pragma: no cover
    unittest.main()
