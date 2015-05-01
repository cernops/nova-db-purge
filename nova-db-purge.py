#!/usr/bin/env python
#
# Copyright (c) 2015 CERN
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Author:
#  Belmiro Moreira <belmiro.moreira@cern.ch>

import argparse
import sys
import ConfigParser
import datetime

from dateutil.parser import *
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func


def makeConnection(db_url):
    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    db_session = Session()
    db_metadata = MetaData()
    db_metadata.bind = engine
    db_base = declarative_base()
    return db_session, db_metadata, db_base


def delete_instance_actions_events(db_meta, instance_uuid):
    instance_actions = Table('instance_actions', db_meta, autoload=True)
    instance_actions_events = Table('instance_actions_events', db_meta, autoload=True)

    instance_actions_ids = select(columns=[instance_actions.c.id], 
        whereclause=and_(instance_actions.c.instance_uuid == instance_uuid)).execute()

    for (action_id,) in instance_actions_ids:
        try:
            instance_actions_events.delete(instance_actions_events.c.action_id == action_id).execute()
        except Exception as e:
            print "ERROR - delete - instance_actions_events with action_id: " + str(action_id)
            print str(e)


def delete_instances(db_meta, instance_uuid):
    instances = Table('instances', db_meta, autoload=True)

    try:
        instances.delete(instances.c.uuid == instance_uuid).execute()
    except Exception as e:
        print "ERROR - delete - instances with uuid: " + str(instance_uuid)
        print str(e)


def delete_others(db_meta, table_name, instance_uuid):
    table = Table(table_name, db_meta, autoload=True)

    try:
        table.delete(table.c.instance_uuid == instance_uuid).execute()
    except Exception as e:
        print "ERROR - delete - " + table_name + " with instance_uuid: " + str(instance_uuid)
        print str(e)


def delete_instance_id_mappings(db_meta, instance_uuid):
    table = Table('instance_id_mappings', db_meta, autoload=True)

    try:
        table.delete(table.c.uuid == instance_uuid).execute()
    except Exception as e:
        print "ERROR - delete - instance_id_mappings with instance_uuid: " + str(instance_uuid)
        print str(e)


def get_instances_by_date(db_meta, date, cell=None):
    instances = Table('instances', db_meta, autoload=True)

    if cell == None:
        instances = select(columns=[instances.c.id, instances.c.uuid, instances.c.created_at, instances.c.deleted_at, instances.c.display_name, instances.c.cell_name],
            whereclause=and_(instances.c.deleted_at < date, instances.c.deleted <> 0)).execute()
    else:
        instances = select(columns=[instances.c.id, instances.c.uuid, instances.c.created_at, instances.c.deleted_at, instances.c.display_name, instances.c.cell_name],
            whereclause=and_(instances.c.deleted_at < date, instances.c.cell_name == cell, instances.c.deleted <> 0)).execute()
    return instances


def get_instances_by_file(file_name, cell=None):
    instances_uuid = []
    f = open(file_name, "r")
    for line in f:
        line = line.rstrip()
        id, uuid, created_at, deleted_at, display_name, cell_name = line.split(',')
        if cell == None:
            instances_uuid.append((id, uuid, created_at, deleted_at, display_name, cell_name))
        elif cell == cell_name:
            instances_uuid.append((id, uuid, created_at, deleted_at, display_name, cell_name))
    f.close()
    return instances_uuid


def confirm_instance_delete_state(db_meta, instance_uuid):
    instances = Table('instances', db_meta, autoload=True)
    a = select([func.count()],
        whereclause=and_(instances.c.uuid == instance_uuid, instances.c.deleted <> 0)).execute()

    (i,) = a.first()
    if i == 0:
        print "ERROR: instance uuid not found or not deleted: " + str(instance_uuid)
        return False
    return True


def purger(db_url, date, file_name, cell, dryrun=False):
    nova_db_session, nova_db_meta, nova_db_base = makeConnection(db_url)
    out_file_name = str(datetime.datetime.utcnow())
    instances_filtered = 0
    instances_deleted = 0
    commits = 0

    tables = [
    'instance_actions',
    'instance_faults', 
    'instance_info_caches',
    'instance_metadata',
    'instance_system_metadata',
    'migrations',
    'virtual_interfaces',
    'block_device_mapping',
    'security_group_instance_association']

    if date: 
        instances = get_instances_by_date(nova_db_meta, date, cell)
    else:
        instances = get_instances_by_file(file_name, cell)

    for (id, uuid, created_at, deleted_at, display_name, cell_name) in instances:
        instances_filtered = instances_filtered + 1
        if uuid==None: 
            uuid='NULL'
        if created_at==None:
            created_at='NULL'
        if deleted_at==None: 
            deleted_at='NULL'
        if display_name==None: 
            display_name='NULL'
        if cell_name==None: 
            cell_name='NULL'

        if not confirm_instance_delete_state(nova_db_meta, uuid):
            continue

        if not dryrun:
            instances_deleted = instances_deleted + 1
            delete_instance_actions_events(nova_db_meta, uuid)
            for table_name in tables:
                delete_others(nova_db_meta, table_name, uuid)
            delete_instance_id_mappings(nova_db_meta, uuid)
            delete_instances(nova_db_meta, uuid)
            if commits > 1000:
                nova_db_session.commit()
                commits = 0
            else: commits = commits + 1

        if file_name == None:
            with open(out_file_name, 'a') as f:
                f.write(str(id) +','+ uuid +','+ str(created_at) +','+ str(deleted_at) +','+display_name +','+ cell_name+'\n')

    if not dryrun: nova_db_session.commit()
    print "Instances filtered: " + str(instances_filtered)
    print "Instances deleted : " + str(instances_deleted)


def get_db_url(config_file):
    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection')
    except:
        print "ERROR: Check nova configuration file."
        sys.exit(2)
    return db_url


def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",
        default="False",
        help="Remove deleted instances until this date")
    parser.add_argument("--file",
        default="False",
        help="Remove deleted instances defined in the file")
    parser.add_argument("--cell",
        default="False",
        help="Remove instances that belong to cell")
    parser.add_argument("--dryrun", 
        help="Don't delete instances",
        action="store_true")
    parser.add_argument("--config",
        help='Configuration file')
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        print("ERROR: Wrong command line arguments")

    if args.date != 'False':
        try:
            parse(args.date)
            date = args.date
        except:
            print("ERROR: Wrong data format")
    else: 
        date = None

    if args.file != 'False':
        try:
            f = open(args.file, "r")
            f.close()
            file_name = args.file
        except:
            print("ERROR: Can't open the file")
    else:
        file_name = None

    if args.cell == 'False':
        cell = None
    else:
        cell = args.cell

    if date == None and file_name == None:
        print("ERROR: Date or File needs to be defined")

    if bool(date) == bool(file_name):
        print("ERROR: Date and File can't be defined simultaneously")

    db_url = get_db_url(args.config)

    purger(db_url, date, file_name, cell, args.dryrun)


if __name__ == "__main__":
    main()
