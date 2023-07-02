"""Helper Classes for User Input UI of 
   Jupyter Notebook Version of Workflow

   INSERT SCRIPT DESCRIPTION HERE
"""


import ipywidgets as wg
from jupyter_ui_poll import ui_events
import time

class commonWidgets:
    """
    INSERT CLASS DESCRIPTION HERE

    Methods
    -------
    submit_button(observer_cmds = None : function, submit_cmds = None : function)
        Creates and displays button that allows users
        to submit their inputs. Also begins UI observer
        that waits for submission button to be pressed
        before more cells in a Jupyter notebook can be run
    
    """
    
    def __init__(self):
        """Create a widget box and populate with a basic text box.
        This "default" widget is only used for testing the class.
        Subclasses will define their own set of widgets in the __init__
        method.
        """

        main_box = wg.VBox((wg.Text(placeholder="Insert text"),))
        self.main_box_copy = main_box
        self.main = wg.Accordion(children=[main_box],
                                 titles=('Default 1',)
                                )


    def submit_button(self, observer_cmds = None, submit_cmds = None):
        """Generates a submit button and assigns a function to
        execute when the button is clicked. Also automatically
        starts the an UI event observer which blocks the cell
        from full execution until the submission button is
        pressed by the user.

        Parameters
        ----------   
        observer_cmds : function (default = None)
                A custom function containing commands
                to execute during each loop of the
                observer. Use in this specific benchmarking
                will focus on disabling/enabling widgets
                that depend on the state of related widgets
                
        submit_cmds : function (default = None)
                A custom function containing commands
                to execute when the user presses the
                submit button. For this benchmarking,
                the parameter will be used to ensure
                that various user inputs are not null

               *****************************************************
               * NOTE: THE `submit_cmds` FUNCTION MUST RETURN A    *
               * BOOLEAN-VALUED FLAG <True or False>. THIS FLAG    *
               * TELLS `on_submit` TO CHANGE THE MAIN SUBMISSION   *
               * FLAG                                              *
               *****************************************************

        Inner Functions
        ---------------
        on_submit(submit_btn : ipywidgets button object)
            Executes custom commands (if specified). If
            no commands are supplied, sets the submission
            flag to true and ends execution of the currently
            running Jupyter notebook cell
            
        event_observer(observer_cmds : function)
            Observes UI events and blocks execution of the
            calling Jupyter notebook cell until the submit
            button is pressed by the user
        """

        # Set the submission flag to false and
        # store submission commands first,
        # since only a button object can be passed
        # into the `on_submit` function.
        self.submit = False
        self.submit_cmds = submit_cmds

        # Define inner functions
        def on_submit(submit_btn):
            # Assume that submission will occur
            cmd_submit_flag = True
            
            # Check for custom submission commands
            if self.submit_cmds != None:
                cmd_submit_flag = self.submit_cmds()
                
            # If `cmd_submit_flag` remains true, set
            # main submission flag to true
            if cmd_submit_flag:
                self.submit = True

                    
        def event_observer(observer_cmds):
            with ui_events() as poll:
                while not self.submit:
                    poll(10) # Polls 10 UI events at a time
                    if observer_cmds != None:
                        observer_cmds()
                    time.sleep(0.1)

        # Create submission button, assign function
        # to execute upon button press, display
        # the button, and launch the observer
        submit_btn = wg.Button(description='Submit')
        submit_btn.on_click(on_submit)
        display(submit_btn)
        event_observer(observer_cmds)


    def add_and_delete_wgs(self):
        """Add description here

        Inner Functions
        ---------------

        """

        def add_widgets(btn):
            children = list(self.main.children)
            children.append(self.main_box_copy)
            self.main.children = children

        def del_widgets(btn):
            current_children = self.main.children
            if len(current_children) > 1:
                self.main.children = current_children[:-1]

        # Create buttons to add and delete widgets and assign corresponding functions
        btns = tuple(wg.Button(description = v) for v in ('Add field', 'Remove field'))
        btns[0].on_click(add_widgets)
        btns[1].on_click(del_widgets)
        btns_box = wg.HBox(btns)
        display(btns_box)

    def display(self):
        self.add_and_delete_wgs()
        display(self.main)
        self.submit_button()
        


class resourceWidgets(commonWidgets):
    """
    Input class description here
    """
    
    # TODO: Allow users to input non-PW resources
    def __init__(self):
        name = wg.Text(placeholder='Resource name')
        scheduler = wg.Dropdown(options=('SLURM',))
        partition = wg.Text(placeholder='Partition name')
        main_box = wg.VBox((name,
                            scheduler,
                            partition
        ))
        self.main_box_copy = main_box
        self.main = wg.Accordion(children=[main_box])

    def display(self):
        self.add_and_delete_wgs()
        display(self.main)
        self.submit_button()


class storageWidgets(commonWidgets):
    """
    Input class description here
    """
    def __init__(self):
        location = wg.Text(placeholder='Storage URI')
        main_box = wg.VBox((location,))

        self.main_box_copy = main_box
        self.main = wg.Accordion(children=[main_box])

    def display(self):
        self.add_and_delete_wgs()
        display(self.main)
        self.submit_button()


class randgenWidgets(commonWidgets):
    """
    Input class description here
    """
    def __init__(self):
        pass