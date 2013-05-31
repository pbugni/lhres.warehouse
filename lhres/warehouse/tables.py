#!/usr/bin/env python

usage = """%prog [options] dbname

Script to generate the tables written to from mirth.

NB: If run as a stand-alone, this will wipe the contents of the named
database and rebuild the tables using the schema structure as defined
in this file, after making SURE that's what you want to do.

Try `%prog --help` for more information.

"""

import sys
import getpass
from sqlalchemy import create_engine
from sqlalchemy import BOOLEAN
from sqlalchemy import CHAR as Char
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import UniqueConstraint 
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import SMALLINT
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy import TEXT
from sqlalchemy import VARCHAR
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relation
from sqlalchemy.orm import sessionmaker

from lhres.util.config import Config
from lhres.util.util import stringFields


metadata = MetaData()

"""
TABLE hl7_raw_message

Contains all HL7 messages in raw form, with only the
message_control_id extracted.
"""
hl7RawMessage_table = Table(
    'hl7_raw_message', metadata,
    Column('hl7_raw_message_id', Integer,
           primary_key=True,
           nullable=False),
    Column('message_control_id', VARCHAR(255), nullable=False,
           index=True, unique=True),
    Column('raw_data', TEXT, nullable=True),
    Column('import_time', TEXT))


class HL7_RawMessage(object):
    def __init__(self, hl7_raw_message_id,
                 message_control_id, raw_data=None):
        self.hl7_raw_message_id = hl7_raw_message_id
        self.message_control_id = message_control_id
        self.raw_data = raw_data
        
    def __repr__(self):
        return '<HL7_RawMessage %s>' % self.hl7_raw_message_id
mapper(HL7_RawMessage, hl7RawMessage_table)
    
"""
TABLE hl7_msh
        
Contains a row for every HL7 message header that comes in.  Only 
houses the data contained in a MSH statement we know to be useful
at this time.  Also hang onto the filename for the batch of
messages being processed - useful for reprocessing, etc.

NB - the hl7_msh_id is the foreign key used by the other HL7_* tables
to link everything that came in from the respective message.  We can't
use the message_control_id or visit_id as these are not unique.  We
get updates and the same message multiple times and need to persist
for resolution of updated data.

"""
hl7Msh_table = Table(
    'hl7_msh', metadata,
    Column('hl7_msh_id', Integer,
           nullable=False, primary_key=True),
    Column('message_control_id', VARCHAR(255), nullable=False,
           index=True),
    Column('message_type', VARCHAR(255), nullable=False),
    Column('facility', VARCHAR(255), nullable=False),
    Column('message_datetime', DateTime, nullable=False),
    Column('batch_filename', VARCHAR(255), nullable=False))

class HL7_Msh(object):
    def __init__(self, hl7_msh_id, message_control_id,
                 message_type, facility, message_datetime,
                 batch_filename):
        self.hl7_msh_id = hl7_msh_id
        self.message_control_id = message_control_id
        self.message_type = message_type
        self.facility = facility
        self.message_datetime = message_datetime
        self.batch_filename = batch_filename

    def __repr__(self):
        return '<HL7_Msh %s>' % self.hl7_msh_id
mapper(HL7_Msh, hl7Msh_table)

"""
TABLE hl7_visit
        
Contains a row for every HL7 message related to a visit

"""
hl7Visit_table = Table(
    'hl7_visit', metadata,
    Column('hl7_visit_id', Integer,
           primary_key=True, nullable=False),
    Column('visit_id', VARCHAR(255), nullable=False, index=True),
    Column('patient_id', VARCHAR(255), nullable=False),
    Column('zip', VARCHAR(12), nullable=True),
    Column('country', Char(3), nullable=True),
    Column('admit_datetime', DateTime, default=None, nullable=True,
           index=True),
    Column('gender', Char(1), default=None, nullable=True),
    Column('dob', VARCHAR(7), default=None, nullable=True),
    Column('chief_complaint', VARCHAR(255), default=None,
           nullable=True),
    Column('patient_class', Char(1), default=None, nullable=True,
           index=True),
    Column('disposition', TEXT, default=None, nullable=True),
    Column('hl7_msh_id', ForeignKey('hl7_msh.hl7_msh_id',
                                    ondelete='CASCADE'), index=True,
           unique=True, nullable=False),
    Column('race', TEXT, default=None, nullable=True),
    Column('county', TEXT, default=None, nullable=True),
    Column('service_code', TEXT, default=None, nullable=True),
    Column('service_alt_id', TEXT, default=None, nullable=True),
    Column('admission_source', TEXT, default=None, nullable=True),
    Column('assigned_patient_location', TEXT, default=None, nullable=True),
    Column('state', Char(2), default=None, nullable=True),
    Column('discharge_datetime', DateTime, default=None, nullable=True,
           index=True),
    )

class HL7_Visit(object):
    def __init__(self, hl7_visit_id, visit_id, patient_id,
                 zip=None, country=None, admit_datetime=None, gender=None,
                 dob=None, chief_complaint=None,
                 patient_class=None, disposition=None,
                 hl7_msh_id=None, race=None, county=None,
                 service_code=None, service_alt_id=None,
                 admission_source=None,
                 assigned_patient_location=None,
                 state=None, discharge_datetime=None):
        self.hl7_visit_id = hl7_visit_id
        self.visit_id = visit_id
        self.patient_id = patient_id
        self.zip = zip
        self.country = country
        self.admit_datetime = admit_datetime
        self.gender = gender
        self.dob = dob
        self.chief_complaint = chief_complaint
        self.patient_class = patient_class
        self.disposition = disposition
        self.hl7_msh_id = hl7_msh_id
        self.race = race
        self.county = county
        self.service_code = service_code
        self.service_alt_id = service_alt_id
        self.admission_source = admission_source
        self.assigned_patient_location = assigned_patient_location
        self.state = state
        self.discharge_datetime = discharge_datetime
        
    def __repr__(self):
        return '<HL7_Visit %s>' % self.hl7_visit_id
mapper(HL7_Visit, hl7Visit_table)
    
"""
TABLE hl7_dx
        
Contains a row for every HL7 DG1 message (diagnosis segment)
related to a visit (i.e. no facility data).

This table should potentially include the diagnosis_datetime, but to
date, we have yet to receive a single non-null diagnosis datetime.
Can't test it, not adding it.

"""
hl7Dx_table = Table(
    'hl7_dx', metadata,
    Column('hl7_dx_id', Integer, primary_key=True),
    Column('dx_code', VARCHAR(255), nullable=True),
    Column('dx_description', TEXT, nullable=True),
    Column('dx_type', Char(1), default=None, nullable=True),
    Column('hl7_msh_id', ForeignKey('hl7_msh.hl7_msh_id',
                                    ondelete='CASCADE'),
           nullable=False, index=True),
    Column('rank', SMALLINT, default=0, nullable=False))

class HL7_Dx(object):
    def __init__(self, hl7_dx_id, dx_code=None,
                 dx_description=None, dx_type=None, hl7_msh_id=None,
                 rank=0):
        self.hl7_dx_id = hl7_dx_id
        self.dx_code = dx_code
        self.dx_description = dx_description
        self.dx_type = dx_type
        self.hl7_msh_id = hl7_msh_id
        self.rank = rank

    def __repr__(self):
        return '<HL7_Dx %s>' % self.hl7_dx_id

    def compare_str(self):
        """Generates a string representation of the instance safe to
        use in comparisons w/ similar types.  (i.e. HL7_Dx and
        Diagnosis)

        """
        return stringFields((self.dx_code,self.dx_type))

mapper(HL7_Dx, hl7Dx_table)

"""
TABLE hl7_obx

Contains a row for every HL7 OBX message (observation result)
related to a visit (i.e. no facility data).

There is a foreign key to the hl7_obr.  In a 'result message' (ORU^R01)
Each ORB is followed by any number of OBX segments.

There are also OBX segments in ADT messages without the OBR association.

"""
hl7Obx_table = Table(
    'hl7_obx', metadata,
    Column('hl7_obx_id', Integer, primary_key=True),
    Column('hl7_obr_id', ForeignKey('hl7_obr.hl7_obr_id',
                                    ondelete='CASCADE'), index=True),
    Column('value_type', Char(3), nullable=True),
    Column('observation_id', VARCHAR(255), nullable=True),
    Column('observation_text', TEXT, nullable=True),
    Column('observation_result', TEXT, nullable=True),
    Column('units', VARCHAR(255), nullable=True),
    Column('result_status', VARCHAR(255), nullable=True),
    Column('observation_datetime', DateTime, nullable=True),
    Column('hl7_msh_id', ForeignKey('hl7_msh.hl7_msh_id',
                                    ondelete='CASCADE'),
           nullable=False, index=True),
    Column('performing_lab_code', VARCHAR(20), default=None,
           nullable=True),
    Column('sequence', VARCHAR(20), default=None,
           nullable=True),
    Column('coding', TEXT, nullable=True),
    Column('alt_id', TEXT, nullable=True),
    Column('alt_text', TEXT, nullable=True),
    Column('alt_coding', TEXT, nullable=True),
    Column('reference_range', TEXT, nullable=True),
    Column('abnorm_id', TEXT, nullable=True),
    Column('abnorm_text', TEXT, nullable=True),
    Column('abnorm_coding', TEXT, nullable=True),
    Column('alt_abnorm_id', TEXT, nullable=True),
    Column('alt_abnorm_text', TEXT, nullable=True),
    Column('alt_abnorm_coding', TEXT, nullable=True),
    )

class HL7_Obx(object):
    def __init__(self, hl7_obx_id=None, hl7_obr_id=None,
                 value_type=None, observation_id=None,
                 observation_text=None,
                 observation_result=None,
                 units=None, result_status=None,
                 observation_datetime=None,
                 hl7_msh_id=None,
                 performing_lab_code=None,
                 sequence=None,
                 coding=None,
                 alt_id=None,
                 alt_text=None,
                 alt_coding=None,
                 reference_range=None,
                 abnorm_id=None,
                 abnorm_text=None,
                 abnorm_coding=None,
                 alt_abnorm_id=None,
                 alt_abnorm_text=None,
                 alt_abnorm_coding=None):
        self.hl7_obx_id = hl7_obx_id
        self.hl7_obr_id = hl7_obr_id
        self.value_type = value_type
        self.observation_id = observation_id
        self.observation_text = observation_text
        self.observation_result = observation_result
        self.units = units
        self.result_status = result_status
        self.observation_datetime = observation_datetime
        self.hl7_msh_id = hl7_msh_id
        self.performing_lab_code = performing_lab_code
        self.sequence = sequence
        self.coding = coding
        self.alt_id = alt_id
        self.alt_text = alt_text
        self.alt_coding = alt_coding
        self.reference_range = reference_range
        self.abnorm_id = abnorm_id
        self.abnorm_text = abnorm_text
        self.abnorm_coding = abnorm_coding
        self.alt_abnorm_id = alt_abnorm_id
        self.alt_abnorm_text = alt_abnorm_text
        self.alt_abnorm_coding = alt_abnorm_coding
        
    def __repr__(self):
        return '<HL7_Obx %s>' % self.hl7_obx_id

    def compare_str(self):
        """Generates a string representation of the instance safe to
        use in comparisons w/ similar types. (i.e. HL7_Obx & Obx)

        """
        # Boolean logic for the value of hl7_obr_id - its value
        # would naturally be different between the two types.
        return stringFields((self.hl7_obr_id and 't' or 'f', 
                         self.value_type,
                         self.observation_id,
                         self.observation_text,
                         self.observation_result,
                         self.units,
                         self.result_status)) 

mapper(HL7_Obx, hl7Obx_table)

"""
TABLE hl7_obr

Contains a row for every HL7 OBR message (observation request)
related to a visit (i.e. no facility data).

"""
hl7Obr_table = Table(
    'hl7_obr', metadata,
    Column('hl7_obr_id', Integer, primary_key=True),
    Column('loinc_code', TEXT, nullable=True),
    Column('loinc_text', TEXT, nullable=True),
    Column('alt_text', TEXT, nullable=True),
    Column('observation_datetime', DateTime, default=None, nullable=True),
    Column('hl7_msh_id', ForeignKey('hl7_msh.hl7_msh_id',
                                    ondelete='CASCADE'),
           nullable=False),
    Column('status', Char(1), default=None, nullable=True),
    Column('report_datetime', DateTime, default=None, nullable=True),
    Column('specimen_source', VARCHAR(20), default=None,
           nullable=True),
    Column('filler_order_no', TEXT, nullable=True),
    Column('coding', TEXT, nullable=True,),
    Column('alt_code', TEXT, nullable=True,),
    Column('alt_coding', TEXT, nullable=True,)
    )

class HL7_Obr(object):
    def __init__(self, hl7_obr_id=None, loinc_code=None,
                 loinc_text=None, alt_text=None,
                 observation_datetime=None, hl7_msh_id=None,
                 status=None, report_datetime=None,
                 specimen_source=None, filler_order_no=None,
                 coding=None, alt_code=None, alt_coding=None):
        self.hl7_obr_id = hl7_obr_id
        self.loinc_code = loinc_code
        self.loinc_text = loinc_text
        self.coding = coding
        self.alt_code = alt_code
        self.alt_text = alt_text
        self.alt_coding = alt_coding
        self.observation_datetime = observation_datetime
        self.hl7_msh_id = hl7_msh_id
        self.status = status
        self.report_datetime = report_datetime
        self.specimen_source = specimen_source
        self.filler_order_no = filler_order_no

    def __repr__(self):
        return '<HL7_Obr %s>' % self.hl7_obr_id

    def compare_str(self):
        """Generates a string representation of the instance safe to
        use in comparisons w/ similar types. (i.e. HL7_Obr & Obr)

        """
        # When we get a loinc_code, we also get loinc_text - don't
        # need both in comparisons.
        return stringFields((self.loinc_code, self.alt_text))

mapper(HL7_Obr, hl7Obr_table,
       properties={'obxes' : relation(HL7_Obx)})

"""
TABLE hl7_nte

Contains a row for every HL7 NTE message (notes and comments segment)
related to an HL7_OBX.

Maintains association with the related HL7_OBX. 

"""
hl7Nte_table = Table(
    'hl7_nte', metadata,
    Column('hl7_nte_id', Integer, primary_key=True),
    Column('sequence_number', SMALLINT, nullable=False),
    Column('note', TEXT, default=None, nullable=True),
    Column('hl7_obx_id', ForeignKey('hl7_obx.hl7_obx_id',
                                    ondelete='CASCADE'), 
           nullable=True, index=True),
    Column('hl7_obr_id', ForeignKey('hl7_obr.hl7_obr_id',
                                    ondelete='CASCADE'),
           nullable=True, index=True),
    )

class HL7_Nte(object):
    def __init__(self, hl7_nte_id=None, sequence_number=None,
                 note=None, hl7_obx_id=None, hl7_obr_id=None):
        self.hl7_nte_id = hl7_nte_id
        self.sequence_number = sequence_number
        self.note = note
        self.hl7_obx_id = hl7_obx_id
        self.hl7_obr_id = hl7_obr_id

    def __repr__(self):
        return '<HL7_Nte %s>' % self.hl7_nte_id


mapper(HL7_Nte, hl7Nte_table)

"""
TABLE hl7_spm

Contains a row for every HL7 SPM message (specimen segment).
Maintains association with the related HL7_OBR. 

"""
hl7Spm_table = Table(
    'hl7_spm', metadata,
    Column('hl7_spm_id', Integer, primary_key=True),
    Column('id', VARCHAR(20), nullable=True),
    Column('description', TEXT, default=None, nullable=True),
    Column('code', VARCHAR(20), default=None, nullable=False),
    Column('hl7_obr_id', ForeignKey('hl7_obr.hl7_obr_id',
                                    ondelete='CASCADE'),
           nullable=False), 
    )

class HL7_Spm(object):
    def __init__(self, hl7_spm_id=None, id=None, description=None,
                 code=None, hl7_obr_id=None):
        self.hl7_spm_id = hl7_spm_id
        self.id = id
        self.description = description
        self.code = code
        self.hl7_obr_id = hl7_obr_id

    def __repr__(self):
        return '<HL7_Spm %s>' % self.hl7_spm_id


mapper(HL7_Spm, hl7Spm_table)

class ObservationData(object):
    """Secondary mapper to make association access easy

    OBR 1 --- * OBX
    --\-1 --- * SPM

    """
    def __repr__(self):
        return '<ObservationData %s>' % self.hl7_obr_id

mapper(ObservationData, hl7Obr_table,
       properties={'obxes' : relation(HL7_Obx,
                                      order_by=HL7_Obx.hl7_obx_id,
                                      lazy=False),
                   'spms' : relation(HL7_Spm)})

class FullMessage(object):
    def __repr__(self):
        return '<FullMessage %s>' % self.hl7_msh_id

# obxes no longer lazy - prevented inclusion of MU data such
# as Chief Complaint and Initial Pulse
mapper(FullMessage, hl7Msh_table,
       exclude_properties=['batch_filename',],
       properties={'visit' : relation(HL7_Visit, lazy=False, uselist=False),
                   'dxes' : relation(HL7_Dx, lazy=False),
                   'obxes' : relation(HL7_Obx, lazy=False)})
# Currently using ObservationData for obr access - save the cycles
#                   'obrs' : relation(HL7_Obr),


"""       properties=dict(\
    obxes=relation(HL7_Obx, primaryjoin=(HL7_Obr.hl7_obr_id ==
                                         HL7_Obx.hl7_obr_id)),
    spms=relation(HL7_Spm, primaryjoin=(HL7_Obr.hl7_obr_id ==
                                        HL7_Spm.hl7_obr_id))))
"""
    
def create_tables(user, password, dbname, enable_delete=False):
    """Create the warehouse database tables.

    :param user: database user with table creation grants
    :param password: the database password
    :param dbname: the database name to populate
    :param enable_delete: testing hook, override for data deletion

    """
    engine = create_engine("postgresql://%s:%s@localhost/%s" %\
                               (user, password, dbname))
    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)

    # Bless the mirth user with the minimal set of privileges
    # Mirth only SELECTs and INSERTs at this time
    config = Config()
    user = config.get('warehouse', 'database_user')
    engine.execute("""BEGIN; GRANT SELECT, INSERT, UPDATE %(delete)s ON
                   hl7_raw_message, hl7_msh, hl7_visit, hl7_dx,
                   hl7_obr, hl7_obx, hl7_nte, hl7_spm TO %(user)s;
                   COMMIT;""" % {'delete': ", DELETE" if enable_delete else '',
                                 'user': user});
    # Sequences also require UPDATE
    engine.execute("BEGIN; GRANT SELECT, UPDATE ON " \
                       "hl7_dx_hl7_dx_id_seq, " \
                       "hl7_msh_hl7_msh_id_seq, "\
                       "hl7_obr_hl7_obr_id_seq, "\
                       "hl7_obx_hl7_obx_id_seq, "\
                       "hl7_nte_hl7_nte_id_seq, "\
                       "hl7_spm_hl7_spm_id_seq, "\
                       "hl7_raw_message_hl7_raw_message_id_seq, "\
                       "hl7_visit_hl7_visit_id_seq TO %(user)s; COMMIT;"\
                       % {'user': user});


def main():  # pragma: no cover
    """Entry point to (re)create the table using config settings"""
    config = Config()
    dbname = config.get('warehouse', 'database')
    print "destroy and recreate database %s ? "\
        "('destroy' to continue): " % dbname,
    answer = sys.stdin.readline().rstrip()
    if answer != 'destroy':
        print "aborting..."
        sys.exit(1)

    user = config.get('warehouse', 'create_table_user')
    print "password for PostgreSQL user:", user
    password = getpass.getpass()
    create_tables(user, password, dbname)


if __name__ == '__main__':  # pragma: no cover
    """ If run as a standalone, recreate the tables. """
    main()
