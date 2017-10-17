
from enums import ObjectType
import os


class TriggerManager(object):
    """ TriggerManager regulates the triggers in the dataframe:
        - giving a space for triggers to live (See other managers for an approach to this)
        - allowing users to add triggers into the manager
        - allowing users to delete triggers into the manager
        - preventing triggers of the same time and action from existing
        - allow the user to add triggers with a specified priority and have that priority take affect with any overlapping triggers
        - allow a way to access triggers saved in the manager
    """

    def __init__(self):
        
        # group key (always string fullname of type) to members of the group.
        # Ex: group key: "Customer" -- > before_insert_customer, after_delete_customer, before_read_customer
        self.trigger_group_members = dict()

    #################################################
    ### API Methods #################################
    #################################################

    def add_trigger(self, trigger_obj):
        # can add a single Trigger class into the trigger_manager into the dataframe

        # Question: Like the type_manager's add_type method, does this method need to return a set of pairs that have been added?

        self.__add_type(trigger_obj)

    def add_types(self, trigger_objs):
        # can add multiple Trigger classes into the trigger_manager into the 
        for obj in trigger_objs:
            self.__add_type(obj)

    def remove_trigger(self, trigger_obj):
        # can remove a Trigger class from the trigger_manager into the dataframe
        pass

    #################################################
    ### Private Methods #############################
    #################################################

    def __add_type(self, trigger_obj):
        """ The manager adds the trigger to a group based on the object type that it is associated with
            - If that associated object type already has a trigger, the manager checks if their is a trigger with the same action and time
                - If it does, then it place the trigger in the group accordingly
                - If it doesn't then it will place the trigger in the list
            - groups are created in dictionaries in the following format: {"associated_pcc_type": {"action+time": trigger_object}}
        """
        # Ex: {"Customer": {"beforeinsert": [before_insert_Customer]}}

        if trigger_obj.type in self.trigger_group_members.keys():
        # Step 1a: Checks if the trigger's pcc type is already in a group (aka - already been added before)

            if trigger_obj.time+trigger_obj.action in self.trigger_group_members[trigger_obj.type].keys():
            # Step 2a: Checks if their are any other triggers of the same time + action
                if trigger_obj.priority != None:
                # Step 3a: Checks if the user declared a priority for the overlapping trigger, and places the trigger in the specified priority
                    self.trigger_group_members[trigger_obj.type][trigger_obj.time+trigger_obj.action].insert(trigger_obj.priority, trigger_obj)
                else:
                # Step 3b: Adds the overlapping trigger to the back of the priority list
                    self.trigger_group_members[trigger_obj.type][trigger_obj.time+trigger_obj.action].append(trigger_obj)
            else:
            # Step 2b: Adds the new trigger to it's pcc type group
                self.trigger_group_members[trigger_obj.type].update({trigger_obj.time + trigger_obj.action: [trigger_obj]})
        else:
        # Step 1b: Creates a group for the trigger's associated type
            self.trigger_group_members.update({trigger_obj.type: {trigger_obj.time + trigger_obj.action: [trigger_obj]}})

