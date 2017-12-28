from rtypes.pcc.triggers import TriggerAction, TriggerTime

from enums import ObjectType
import os
import bisect


class TriggerManager(object):
    """Used to regulate all trigger's that exist in the dataframe.
       Can do the following:
       - Add triggers
       - Get triggers 
       - Delete triggers 
       - Execute triggers

    Attributes:
        trigger_map (dict): Dictionary used to map out all triggers in the manager.
            Format: {pcc_type: {time + action: [TriggerProcedures]}}
    """

    def __init__(self):
        """Creates a TriggerManager object, and initializes a trigger_map. 
        """
        self.trigger_map = dict()

    #################################################
    ### API Methods #################################
    #################################################

    def add_trigger(self, trigger_obj):
        """Method used to add a single TriggerProcedure obj into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        self.__add_trigger(trigger_obj)

    def add_triggers(self, trigger_objs):
        """Method used to add multiple TriggerProcedure objs into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        for obj in trigger_objs:
            self.__add_trigger(obj)

    def execute_trigger(self, tp_obj, time, action, dataframe, new, old, current):
        """Method used to execute one specific TriggerProcedure obj. 
           Only executes TriggerProcedure objs that meet the specified criteria.
           Passes in arguments "dataframe", "new", "old", and "current" into the procedure

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger
                dataframe (???): n/a
                new (???): n/a
                old (???): n/a
                current (???): n/a

            Returns:
                None: Does not return anything, only activates procedure objects
        """
        # What arguments would I need in order to allow the trigger to execute ???????
        self.__execute_trigger(tp_obj, time, action, dataframe, new, old, current)
        return True # CHANGE THIS WHEN DONE TESTING ??????????????????????????????????????

    def remove_trigger(self, trigger_obj):
        """Method used to remove TriggerProcedure objs from the trigger_map.
           Preventing the procedure from being activated.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to determine if the TriggerProcedure exist in
                    the trigger_map. Then used to delete the TriggerProcedure.

            Returns:
                None: Does not return anything, only deletes TriggerProcedure obj
        """
        self.__remove_trigger(trigger_obj)

    def trigger_exists(self, tp_obj, time, action):
       return self.__trigger_in_map(tp_obj, time, action)

    #################################################
    ### Private Methods #############################
    #################################################

    def __add_trigger(self, trigger_obj):
        """Method used to add a single TriggerProcedure obj into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        bisect.insort(
            self.trigger_map.setdefault(
                trigger_obj.pcc_type,
                dict()).setdefault(
                    trigger_obj.time + trigger_obj.action,
                    list()),
            trigger_obj)

    def __get_trigger(self, tp, time, action):
        """Method used to get a TriggerProcedure obj attached to a PCC Type.

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger

            Returns:
                list: This is a list of TriggerProcedure objs that are associated with
                      the specified type and activated at the specified time + action
        """

        ##################################################################################
        # This needs to be changed! For some reason, triggers are placed in reverse order
        ##################################################################################

        if tp in self.trigger_map and ((time + action) in self.trigger_map[tp]):
        # 1a: Check if the pcc_type has any triggers atteched to it
            self.trigger_map[tp][time + action].reverse()
            return self.trigger_map[tp][time + action]
        else:
        # 1b: Return an empty list
            return list()

    def __execute_trigger(self, tp, time, action, dataframe, new, old, current):
        """Method used to execute speciific TriggerProcedure objs. 
           Only executes TriggerProcedure objs that meet the specified criteria.
           Passes in arguments "dataframe", "new", "old", and "current" into the procedure

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger
                dataframe (???): n/a
                new (???): n/a
                old (???): n/a
                current (???): n/a

            Returns:
                None: Does not return anything, only activates procedure objects
        """
        try:
            for procedure in self.__get_trigger(tp, time, action):
                procedure(dataframe=dataframe, new=new, old=old, current=current)
        except KeyError:
            raise

    def __remove_trigger(self, trigger_obj):
        """Method used to remove TriggerProcedure objs from the trigger_map.
           Preventing the procedure from being activated.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to determine if the TriggerProcedure exist in
                    the trigger_map. Then used to delete the TriggerProcedure.

            Returns:
                None: Does not return anything, only deletes TriggerProcedure obj
        """
        # If the trigger exist in the trigger_map, remove the Procedure from the map
        if self.__trigger_obj_in_map(trigger_obj):
            self.__get_trigger(trigger_obj.pcc_type,
                               trigger_obj.time,
                               trigger_obj.action).pop(
                                   self.__procedure_index_location(trigger_obj))
        else:
            raise ValueError(str(trigger_obj.pcc_type)
                             + " does not have any triggers attached to the dataframe")

    def __procedure_index_location(self, trigger_obj):
        """Method determines index position of a TriggerProcedure object in the 
           trigger_map's list of TriggerProcedure objs.

            Args:
                trigger_obj (TriggerProcedure):
                    Used to determine location of the TriggerProcedure object

            Returns:
                int: represents the index location of the TriggerProcedure in the
                     trigger_map's list of TriggerProcedure objs.
        """
        # Had to do this because the .index method wasn'tm working, as a result of
        # changing the __eq__ method

        for index in range((len(
            self.trigger_map[trigger_obj.pcc_type][trigger_obj.time
                                                   + trigger_obj.action]))):
            if (self.trigger_map[
                trigger_obj.pcc_type][trigger_obj.time
                                      + trigger_obj.action][index].procedure.__name__ 
                == trigger_obj.procedure.__name__):
                return index

    def __trigger_obj_in_map(self, trigger_obj):
        """Method determines if the TriggerProcedure obj exist in the trigger_map.

            Args:
                trigger_obj (TriggerProcedure):
                    Used to determine if the TriggerProcedure obj exist in the trigger_map

            Returns:
                bool: True if the TriggerProcedure exist in the trigger_map, else False
        """
        return self.__trigger_in_map(
            trigger_obj.pcc_type, trigger_obj.time, trigger_obj.action)

    def __trigger_in_map(self, tp, time ,action):
        try:
            return len(self.trigger_map[tp][time + action]) != 0
        except KeyError:
            return False
