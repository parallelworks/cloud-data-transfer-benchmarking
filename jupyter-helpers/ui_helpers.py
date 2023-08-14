"""Helper Classes for User Input UI of 
   Jupyter Notebook Version of Workflow

   This script defines helper classes that generate
   input widgets for the `cloud-data-transfer-benchmarking`
   workflow (found in PW GH under a repository of the same
   name). These classes are for the interactive version 
   of this workflow (i.e., running from a Jupyter notebook), 
   allowing a user to input all of the required parameters
   to make the workflow function.

   Classes
   -------
   commonWidgets
        This class contains all widget functionality that is
        common between specific input widgets. It includes
        methods to add and delete fields (such as a desired
        setup that requires multiple resources or cloud
        storage locations) and a submit button that confirms
        and displays inputs
    
    resourceWidgets(commonWidgets)
        Inherited from commonWidgets. Sets up and generates widgets
        that allow user to input cloud compute resources and accompanying
        information required for workflow execution.
    
    storageWidgets(commonWidgets)
        Inherited from commonWidgets. Similarly to resourceWidgets, sets
        up widgets for cloud object storage input information.

    randgenWidgets(commonWidgets)
        Inhertited from commonWidgets. Sets up widgets that display options
        for randomly generating files. This is an optional feature, but can
        be used whenever a user doesn't have/doesn't want to supply their own
        dataset.

    userdataWidget(commonWidgets)
        Inhertited from commonWidgets. Sets up widgets that allow users to
        input their own data sets. Data may be stored in cloud storage already,
        or could be on the local filesystem.

    convertOptions(commonWidgets)
        Used to define options for applying different compression algorithms and chunksizes to
        datasets


   Boxes refers to the groups that widgets are placed
   in. In these helper classes, a box is referred to
   a container object that holds widgets or other boxes.
   The following hierarchy will apply to all classes in
   this helper file:

        I. main
        An `ipywidget` accordion-style grouping that
        holds other boxes.
                II. main_box
                Boxes that make up groups of widgets. The box
                for a particular part of the UI (i.e., resource
                info, storage info, and randomly generated file
                options)
                        III. widget
                        Individual component widgets of the UI
                        (such as buttons, text boxes, check boxes, etc.)

ipywidgets is the package used to generate all widgets to `main.ipynb`,
jupyter-ui-poll is required to observe events happening in the UI and
check the states of specified widgets, and time is needed as a utility
to the widget observer set up by jupyter-ui-poll. All packages must
be installed on the environment the Jupyter notebook kernel is referencing.
`main.ipynb` will automatically check for the correct version of these packages
and install them if they are not there already.        
"""
import ipywidgets as wg
from jupyter_ui_poll import ui_events
import time
import os
import subprocess



class commonWidgets:
    """
    Class that defines commonly used functionality for user input UI. These include
    functions that allow a user to dynamically add new input fields and initialize
    a submit button that allows for input submission.

    Methods
    -------
    widget_func():
        A function that defines widgets that need to have a fresh copy for adding
        more input fields are deleting them. This function does not need to be defined
        if `AppendBoxes()` is not called anywhere in the `display()` method.

    submit_button(observer_cmds = None : function, submit_cmds = None : function):
        Creates and displays button that allows users
        to submit their inputs. Also begins UI observer
        that waits for submission button to be pressed
        before more cells in a Jupyter notebook can be run

    AppendBoxes(box_title = None):
        Allows users to dynamically add or delete widgets for inputs that may require
        more than one field (e.g., cloud computing clusters, cloud storage locations).
        Note that, if called anywhere in the class, the method `widget_func()` must be defined.
    """
 
    def __init__(self):
        """Create a widget box and populate with a basic text box.
        This "default" widget is only used for testing the class.
        Subclasses will define their own set of widgets in the __init__
        method.
        """

        main_box = self.widget_func()
        self.main = wg.Accordion(children=[main_box],
                                 titles=('Default 1',)
                                )

    def widget_func(self):
        widget = wg.VBox((wg.Text(placeholder="Insert text"),))
        return widget

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

        Returns
        -------
        submit_btn : ipywidgets button object
            The submit button object
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

        return submit_btn


    def AppendBoxes(self, box_title = None):
        """Method that introduces the functionality
        of appending copies of `main_box` into `main`.
        Should not be called individually, as the function
        depends on having the correct widgets configured.

        Parameters
        ----------
        box_title : string (default = None)
            Title to give a newly appended accordion box. If no
            title is passed, new boxes will not have one.

        Inner Functions
        ---------------
        Note: The following two functions have their parameters
        passed in automatically upon their call in this method.
    
        add_widgets(btn : ipywidgets button object)
            Appends a `main_box` copy to `main`. Copy is in the
            base state (i.e., all widgets are in the same state
            as they were upon initial creation)
        del_widgets(btn : ipywidgets)
            Deletes data from and removes the last copy of `main_box`
            from `main` that was created with `add_widgets`

        Returns
        -------
        btns_box : ipywidgets box object
            Box that contains the add and delete fields
            buttons
        """

        # Function that creates a copy of the widgets defined in
        # `widget_func()` and adds it to the ipywidgets accordion
        # object
        def add_widgets(btn):
            children = list(self.main.children)
            main_box_copy = self.widget_func()
            children.append(main_box_copy)
            titles = list(self.main.titles)
            titles.append(box_title + ' ' + str(len(children)))
            self.main.children = children
            self.main.titles = titles

        # Removes the last widget added to the accordion object.
        # Will not remove a widget if there is only one remaining
        # in the accordion.
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

        return btns_box
        

    def display(self):
        """Displays widgets defined in `__init__`
        and keeps a running total of all widgets displayed
        at any given point. Also initializes the submit button
        and all objects associated with it.
        """
        # Initialize widget list
        wg_lst = [self.main]

        # Add widget appending functionality to the
        # UI to be displayed
        btn_box = self.AppendBoxes()
        wg_lst.append()

        # Display the main set of widgets
        display(self.main)

        # Initialize the submit button. Starts up
        # the observer execution of the code below
        # this line continues when submission button
        # is pressed
        submit_btn = self.submit_button()
        wg_lst.append()

        # Close all widgets in the list
        [v.close() for v in wg_lst]
        


class resourceWidgets(commonWidgets):
    """
    Widgets designed to allow users to fill in
    all required information pertaining to cloud resources.
    New fields can be added very simply by editing `widget_func()`
    and `process_input()`

    Methods
    -------
    widget_func():
        Defines the widgets that can be added or removed with `AppendWidgets()`
        inherited from the `commonWidgets` class
    
    display():
        See `display()` in `commonWidgets` for basic information about this class.
        Added functionality includes defining conditions to check upon clicking the
        submission button.
    """
    
    # TODO: Allow users to input non-PW resources
    def __init__(self):
        "Initializes the main body of functions upon instantiation of the class"
        main_box = self.widget_func()
        self.main = wg.Accordion(children=[main_box], titles = ("Resource 1",))


    def widget_func(self):
        "Defines the widgets that will be used in this particular field of inputs"
        widget = wg.VBox((wg.HBox([wg.Label('Resource Name: '),
                                 wg.Text()
                                ]),
                        wg.HBox([wg.Label('Public IP: '),
                                wg.Text()
                            ]),
                        wg.HBox([wg.Label('Cloud Service Provider: '),
                                wg.Dropdown(options=('GCP', 'AWS'))
                            ]),
                        wg.HBox([wg.Label('Resource Manager: '),
                                wg.Dropdown(options=('SLURM',))
                            ]),
                        wg.HBox([wg.Label('Partition Name: '),
                                wg.Text(value='compute')
                            ]),
                        wg.HBox([wg.Label('CPUs/node: '),
                                wg.IntText()
                            ]),
                        wg.HBox([wg.Label('Memory/node (GB): '),
                                wg.FloatText()
                                ]),
                        wg.HBox([wg.Label('Miniconda Directory: '),
                                wg.Text(value='~')
                                ])
                        ))
        return widget

    def display(self):
        "See `display()` in `commonWidgets` for information on this class"
        box_list = [self.main]

        # Defines a function to pass to the submit button method. Checks to
        # see if any displayed input fields are blank and blocks submission
        # of the inputs if they are.
        def submit_cmds():
            boxes = self.main.children

            for n in boxes:
                for i in n.children:
                    if len(str(i.children[1].value)) == 0:
                        print('Ensure no fields are blank before submitting.')
                        return False
                    elif str(i.children[1].value)[0] == '0':
                        print('Ensure all integer fields are nonzero and/or do not have leading zeros.')
                        return False
            
            return True

        # Same scheme from `commonWidgets.display()`
        btn_box = self.AppendBoxes(box_title = 'Resource')
        box_list.append(btn_box)
        display(self.main)
        submit_btn = self.submit_button(submit_cmds=submit_cmds)
        box_list.append(submit_btn)
        [v.close() for v in box_list]


    def processInput(self):
        """When this class is called from the Jupyter notebook, the input
        information stored from the `display()` method is parsed and formatted
        into a list of dictionaries containing all user input fields. At the moment,
        only PW resources may be used in the benchmarking. In future updates, users
        will be able to run this workflow from outside the PW platform by passing
        information about the host and IP of the head node of a cluster

        Returns
        -------
        resources : list(dict)
            Stores user inputs of cloud resource information and is output
            to the calling Jupyter notebook.
        """
        resources = []
        boxes = self.main.children
        # Loop through all boxes and store the information about
        # each resource
        for i in boxes:
            children = i.children
            resources.append({"Name" : children[0].children[1].value,
                            "IP" : children[1].children[1].value,
                            "MinicondaDir" : children[7].children[1].value,
                            "CSP" : children[1].children[2].value,
                            "Dask" : {"Scheduler" : children[3].children[1].value,
                                        "Partition" : children[4].children[1].value,
                                        "CPUs" : children[5].children[1].value,
                                        "Memory" : children[6].children[1].value
                                        }
                                              })

        print('\n-----------------------------------------------------------------------------')
        print('If you wish to change information about cloud resources, run this cell again.\n')
        return resources



class storageWidgets(commonWidgets):
    """
    Similar to `resourceWidgets`, this class takes user inputs about cloud storage locations.
    The main difference is in `widget_func()`, where a different set of widgets is defined
    to be added and deleted once the ui is running.

    Methods
    -------
    widget_func():
        See `resourceWidgets.widget_func()` and `commonWidgets.widget_func()` for basic information.
        This particular version of the method makes a single text box allowing the user
        to input the cloud object storage location URI or mount path (if using PW storage)

    display():
        See the `resourceWidgets` and `commonWidgets` classes for basic information. This instance of
        the `display()` method is identical to the one seen in `resourceWidgets`.

    processInput():
        Stores storage information input by user and returns the formatted data
        to the calling Jupyter notebook

    """

    def __init__(self):
        "Initialize first widget box"
        main_box = self.widget_func()
        self.main = wg.Accordion(children=[main_box],
                                titles = ('Cloud Object Store 1',))

    def widget_func(self):
        "Set widgets specific to cloud object storage for copying"
        widget = wg.VBox((wg.HBox([wg.Label('Storage URI: '),
                                wg.Text(placeholder="gcs://my-bucket or s3://my-bucket")
                                ]),
                        wg.HBox([wg.Label('Bucket Type'),
                                wg.Dropdown(options=('Public',
                                                    'Private'))
                                ]),
                         wg.HBox([wg.Label('Cloud Service Provider: '),
                                wg.Dropdown(options=('GCP', 'AWS'))
                                ]),
                        wg.VBox([wg.Label('Credentials: '),
                                wg.Text(disabled = True),
                                wg.Password(disabled = True),
                                ])
                        ))
        return widget

    def display(self):
        "Identical code to the `.display()` method in `resourceWidgets`"
        box_list = [self.main]

        def observer_cmds():
            data = self.main.children
            for i in data:
                children = i.children
                path = children[0].children[1].value
                bucket_type = children[1].children[1].value
                csp = children[2].children[1]
                credentials = children[3]
                credentials_desc = children[3].children[0].value

                # Determine csp from URI
                uri_prefix = path.split(':')[0]
                if uri_prefix == 'gs' or uri_prefix == 'gcs':
                    csp.disabled = True
                    csp.value = 'GCP'
                elif uri_prefix == 's3':
                    csp.disabled = True
                    csp.value = 'AWS'
                else:
                    csp.disabled = False

                # Determine value of credentials input field based on bucket type and CSP
                if bucket_type == 'Private':
                    match csp.value:
                        case 'GCP':
                            credentials.children[1].disabled = False
                            credentials.children[2].disabled = True
                            credentials.children[1].placeholder = '/path/to/token/file.json'
                            credentials.children[2].placeholder = ''
                        case 'AWS':
                            for cred in credentials.children[1:]:
                                cred.disabled = False
                            credentials.children[1].placeholder = 'AWS_ACCESS_KEY_ID'
                            credentials.children[2].placeholder = 'AWS_SECRET_ACCESS_KEY'
                else:
                    for cred in credentials.children[1:]:
                        cred.disabled = True
                        cred.value = ''
                        cred.placeholder = ''



        def submit_cmds():
            boxes = self.main.children

            for n in boxes:
                for i in n.children:
                    if i.children[1].value == n.children[3].children[1].value and n.children[1].children[1].value != 'Private':
                        pass

                    elif i.children[1].value == n.children[3].children[1].value and n.children[1].children[1].value == 'Private':
                        match n.children[2].children[1].value:
                            case 'GCP':
                                file = i.children[1].value

                                if not os.path.isfile(file):
                                    print('Credentials file not found. Ensure you have input the correct path.')
                                    return False

                    elif len(i.children[1].value) == 0:
                        print('Ensure no fields are blank before submitting.')
                        return False
            
            return True

        btn_box = self.AppendBoxes(box_title='Cloud Object Store')
        box_list.append(btn_box)
        display(self.main)
        submit_btn = self.submit_button(observer_cmds = observer_cmds, submit_cmds = submit_cmds)
        box_list.append(submit_btn)
        [v.close() for v in box_list]

    def processInput(self):
        "New inputs can be defined and added on as the workflow develops"
        storage = []
        data = self.main.children
        for i in data:
            children = i.children
            path = children[0].children[1].value
            csp = children[2].children[1].value
            bucket_type = children[1].children[1].value

            if path[-1] == '/':
                path = path[:-1]

            if csp == 'AWS' and bucket_type == "Private":
                credentials = {'anon' : False}
                labels = ['key', 'secret', 'session']
                for index, child in enumerate(children[3].children[1:]):
                    credentials[labels[index]] = child.value
            elif csp == 'GCP' and bucket_type == "Private":
                credentials = {'token':children[3].children[1].value}
            else:
                credentials = {'anon' : True}

            storage.append({"Path" : path,
                            "Type" : bucket_type,
                            "CSP" : csp,
                            "Credentials" : credentials})

        print('\n--------------------------------------------------------------------------------------')
        print('If you wish to change information about cloud storage locations, run this cell again.\n')
        return storage



class randgenWidgets(commonWidgets):
    """
    Creates widgets that allow user to define randomly generated file options.

    Methods
    -------
    processInput():
        Formats user input about randomly generated files and outputs back to
        the calling Jupyter notebook
    """
    def __init__(self, resources = None):
        """
        Initializes all widgets pertaining to randomly generated files. Currently,
        only generation of CSV, NetCDF4, and Binary files are supported by the workflow.
        There is no `widget_func()` method like previous classes because these widgets
        do not need to be dynamically added and removed with `AppendWidgets()`.

        Parameters
        ----------
        resources : list(dict(...)) (default = None)
            The data output from `resourceWidgets`. This will be used
            to gather resource names that will serve as resource selections
            to choose from in the randomly generated options. If no options
            are passed in, will return a message indicating that the the
            resources input section must be completed first
        """

        resource_names = []
        # Check if resources have not been specified (this should never trigger if the user
        # doesn't edit any code in `main.ipynb`)
        if resources == None:
            print('Enter your inputs in the `\"Cloud Compute Resources\" section before' \
            'filling in the randomly generated file options.')

        # Pull resource names from resource input data
        for i in resources:
            resource_names.append(i['Name'])

        # Create recurring widgets in functions
        def create_checkbox(label='Generate?'):
            wig = wg.HBox([wg.Label(label), wg.Checkbox()])
            return wig
        
        def create_sizebox(label='File Size (GB): ', value=0):
            wig = wg.HBox([wg.Label(label), wg.FloatText(disabled=True, value=value)])
            return wig

        # Dropdown box that allows users to choose a resource to write randomly generated
        # files with
        rand_resource = wg.HBox([wg.Label('Resource to write random files with: '), wg.Dropdown(options = resource_names)])

        # Checkbox and filesize box for CSV files. Checking the box indicates that the user
        # wants to generate the file. Assume this behavior for all checkboxes in this section
        csv_checkbox = create_checkbox()
        csv_sizebox = create_sizebox()
        csv_box = wg.VBox([csv_checkbox, csv_sizebox])

        # Similar to CSV file widgets, except adds fields to let the user set the dimensionality
        # of the NetCDF4 file. Currently, only supports float axes and time axes.
        netcdf_checkbox = create_checkbox()
        netcdf_sizebox = create_sizebox()
        datavars = create_sizebox(label='Data Variables: ', value=1)
        floatax = create_sizebox(label='Axes (dtype = float64): ', value=2)
        netcdf_box = wg.VBox([netcdf_checkbox,
                              netcdf_sizebox,
                              datavars,
                              floatax
                            ])

        # Same widgets as CSV file widgets
        # binary_checkbox = create_checkbox()
        # binary_sizebox = create_sizebox()
        # binary_box = wg.VBox([binary_checkbox,
        #                       binary_sizebox
        #                     ])
        
        # Populate main box with all randomly generated file widgets
        main_box = wg.Tab(children=[csv_box, netcdf_box],#, binary_box],
                          titles=('CSV', 'NetCDF4'))#, 'Binary'))

        # Place all widgets defined in this section into accordion object for display
        self.main = wg.Accordion(children=[rand_resource, main_box],
                                titles=('Select Resource', 'Select Files to Generate'))

    def processInput(self):
        """Similar to all other `processInput()` methods, but specific to randomly
        generated files

        Returns
        -------
        out : list(dict)
            List containing a single dictionairy that stores all information
            about randomly generated files
        """

        # Grab the strings containing the file format names
        formats = self.main.children[1].titles

        # Get all individual widgets from each file format
        fields = []
        for i in range(len(formats)):
            fields.append(tuple(self.main.children[1].children[i].children[0:]))

        # Since no fields are dynamically added or subtracted, locations of specific
        # information are hardcoded in
        out = [{"Format" : formats[0],
                "Generate" : fields[0][0].children[1].value,
                "SizeGB" : fields[0][1].children[1].value
                },
                {"Format" : formats[1],
                "Generate" : fields[1][0].children[1].value,
                "SizeGB" : fields[1][1].children[1].value,
                "Data Variables" : fields[1][2].children[1].value,
                "Float Coords" : fields[1][3].children[1].value,
                },
                
                # TODO: Uncomment lines when Binary random file generation is fully-supported
                # {"Format" : formats[2],
                # "Generate" : fields[2][0].children[1].value,
                # "SizeGB" : fields[2][1].children[1].value
                # },
                {"Resource" : self.main.children[0].children[1].value}
              ]

        print('\n-------------------------------------------------------------------------------')
        print('If you wish to change the randomly generated file options, run this cell again.\n')
        return out


    def display(self):
        """
        Similar to all other display methods, but does not call the `AppendWidgets()` method
        and also defines commands to pass to the observer"""

        wg_lst = [self.main]

        # During each iteration of the observer loop, checks to see if the CheckBoxes
        # for each file type are activated. If the checkbox is not checked, the widgets
        # under it will be disabled. If checked, all widgets for a particular file format
        # will allow the user to input information.
        def observer_cmds():
            files = self.main.children[1].children
            check_status = tuple(v.children[0].children[1].value for v in files)
            disable_status = tuple(v.children[1].children[1].disabled for v in files)

            for i in range(len(check_status)):
                fields = self.main.children[1].children[i].children[1:]
                if check_status[i] == False and disable_status[i] == False:
                    for n in fields:
                        n.children[1].disabled = True
                elif check_status[i] == True and disable_status[i] == True:
                    for n in fields:
                        n.children[1].disabled = False
    
        # If a checkbox for a particular file type is checked, searches through
        # filesize widgets and will not submit the inputs if any fields are zero.
        # For NetCDF4 dimension variables, will not submit the inputs if any of those
        # are zero.
        def submit_cmds():
            # Assume that all inputs are ok
            cmd_submit_flag = True

            # Grab information about checkbox states and file sizes corresponding
            # to each file type
            files = self.main.children[1].children
            filetype_bool = [v.children[0].children[1].value for v in files]
            filetype_desc = self.main.children[1].titles
            filesize = [v.children[1].children[1].value for v in files]
            # Find the index of the filetypes that are true
            true_index = [i for i, v in enumerate(filetype_bool) if v == True]

            # Get info about the extra NetCDF4 options
            nc_info = files[1].children[2:]
            nc_info_vals = [v.children[1].value for v in nc_info]

            # Loop through the indexes of the filetypes that are set
            # to be generated
            for i in true_index:
                # If any filesize is zero, block submission and indicate
                # which file the error corresponds to
                if not filesize[i]:
                    print(filetype_desc[i], 'must have nonzero size.')
                    cmd_submit_flag = False

                # If NetCDF4 is set to be created, check if any extra
                # parameters are zero and block execution if they are
                match filetype_desc[i]:
                    case "NetCDF4":
                        if not all(nc_info_vals):
                            print('NetCDF4 size options must be nonzero')
                            cmd_submit_flag = False

            return cmd_submit_flag


        display(self.main)
        submit_btn = self.submit_button(observer_cmds=observer_cmds, submit_cmds=submit_cmds)
        wg_lst.append(submit_btn)

        [v.close() for v in wg_lst]





class userdataWidgets(commonWidgets):
    """This class generates widgets that allow users to input information
    about their own datasets for use in the benchmarking. Users input
    the file format, location of the data (either one that exists already)
    or one outside the scope of the benchmarking, and other accompanying
    information.

    Attributes
    ----------
    storage : list(dict) (default = None)
        This parameter is important for passing information about previously-defined
        cloud object stores to the widget. In the event the user chooses one of these
        stores, all other options will be immediately filled in

    Methods
    -------
    widget_func():
        Defines the suite of widgets that will be used to generate this part of the UI.
    display():
        displays and runs the widgets defined, adding the functionality of add and delete
        buttons, as well as a submit button
    processInput():
        Gathers the input recieved upon pressing the submit button and formats it into a
        dictionary output
    """


    def __init__(self, storage=None):
        "Initilaize class with information about previous storage and setup widget box"

        # Get information about storage locations the user has already defined
        self.storage = storage
        self.storage_names = [store['Path'] for store in storage]

        self.checkbox = wg.Checkbox(description='Provide datasets to workflow?')
        main_box = self.widget_func()
        self.main = wg.Accordion(children=[main_box], titles = ("User Dataset 1",), disabled = True)



    def widget_func(self):
        "Defines the widgets that will be used in this particular field of inputs"
        widget = wg.VBox((wg.HBox([wg.Label("Data Format"),
                                wg.Dropdown(options=('NetCDF4',
                                                    'CSV',
                                                    #'Binary',
                                                    'Zarr',
                                                    'Parquet'))
                                    ]),
                        wg.HBox([wg.Label("Storage Location: "),
                                wg.Dropdown(options=tuple(self.storage_names + ['Local Filesystem']))
                                ]),
                        wg.HBox([wg.Label("URI/Path of Data: "),
                                wg.Text()
                                ]),
                        wg.VBox([wg.Label('Data Variables (Type * to run benchmarking with all data variables in the dataset or separate names with a comma to use multiple): '),
                                wg.Text(value = '*')
                                ])
                        ))
        return widget



    def display(self):
        "Identical code to the `.display()` method in `resourceWidgets`"
        box_list = [self.main]


        # Observer commands are more complicated than other classes. See following
        # comments for more information
        def observer_cmds():
            data = self.main.children

            for i in data:
                # Store one aspect of the UI in each variable
                children = i.children
                storage_location = children[1].children[1].value
                uriOrPath = children[2].children[1]
                data_format = children[0].children[1].value
                data_variable = children[3].children[1]

                # First, check the storage location. The placeholder for text
                # boxes changes based on which storage location is selected.
                match storage_location:
                    case 'Local Filesystem':
                        uriOrPath.placeholder='/path/to/data'
                    case other:
                        uriOrPath.placeholder = 'path/in/bucket/to/data'

                if data_format == 'NetCDF4' or data_format == "Zarr":
                    data_variable.disabled = False
                else:
                    data_variable.disabled = True
                    data_variable.value = ''

        # END INNER FUNCTION

        def submit_cmds():
            boxes = self.main.children

            if self.checkbox.value:
                for n in boxes:
                    for i in n.children:
                        if len(i.children[1].value) == 0:
                            if n.children[0].children[1] == 'CSV' or n.children[0].children[1] == 'Parquet':
                                pass
                            else:
                                print('Ensure no fields are blank before submitting.')
                                return False
            
            return True
        # END INNER FUNCTION

        display(self.checkbox)
        box_list.append(self.checkbox)
        btn_box = self.AppendBoxes(box_title='User Dataset')
        box_list.append(btn_box)
        display(self.main)
        submit_btn = self.submit_button(observer_cmds = observer_cmds, submit_cmds = submit_cmds)
        box_list.append(submit_btn)
        [v.close() for v in box_list]



    def processInput(self):
        "New inputs can be defined and added on as the workflow develops"
        user_data = []
        data = self.main.children

        # If the checkbox to use user input files is not checked,
        # set inputs as empty (even if fields are filled in)
        # TODO: Find a better way to do this
        if not self.checkbox.value:
            pass
        else:
            for i in data:
                children = i.children
                storage_location = children[1].children[1].value
                path = children[2].children[1].value
                data_vars = children[3].children[1].value

                # First check to see if the storage location
                # selected by the user matches those previously
                # defined in the input process. If so, grab the
                # bucket type, credentials, and csp from those
                # storage locations
                for i in self.storage:
                    if storage_location == i['Path']:
                        bucket_type = i['Type']
                        credentials = i['Credentials']
                        csp = i['CSP']

                        # If the user put a `/` in front of the bucket
                        # path to the data, remove it
                        if path[0] == '/':
                            path = path[1:]

                        source_path = f"{storage_location}/{path}"
                        break

                # If storage location is local or other cloud storage,
                # set bucket type, credentials, and csp depending on
                # on user inputs
                if storage_location == 'Local Filesystem':
                    bucket_type = 'Local'
                    source_path = path
                    csp = 'Local'
                    credentials = ''


                # Populate dictionary with fields
                user_data.append({"Format" : children[0].children[1].value,
                                "SourcePath" : source_path,
                                "DataVars" : data_vars.split(','),
                                "Type" : bucket_type,
                                "CSP" : csp,
                                "Credentials" : credentials})

        print('\n---------------------------------------------------------------------------')
        print('If you wish to change information about your input data, run this cell again.\n')
        return user_data




class convertOptions(commonWidgets):


    def __init__(self, userfiles, randfiles):

        self.gridded_datasets = []
        self.tabular_datasets = []

        for file in randfiles[:-1]:
            if file['Generate']:
                size = file['SizeGB']
                fformat = file['Format']
                if fformat == 'CSV':
                    self.tabular_datasets.append(f'random_{float(size)}GB_CSV')
                elif fformat == 'NetCDF4':
                    self.gridded_datasets.append(f'random_{float(size)}GB_NetCDF4.nc')

        for file in userfiles:
            fformat = file['Format']

            sourcepath = file['SourcePath']
            split_path = [ele for ele in sourcepath.split('/') if len(ele)!=0]
            path = split_path[-1]
            if path == '*':
                path = split_path[-2]

            if fformat == 'CSV':
                if not path in self.tabular_datasets:
                    self.tabular_datasets.append(path)
            elif fformat == 'NetCDF4':
                if not path in self.gridded_datasets:
                    self.gridded_datasets.append(path)


        main_box = self.widget_func()
        self.main = wg.Accordion(children=[main_box], titles = ("Cloud Native Data Option Set 1",), disabled = True)



    def widget_func(self):
        "Defines the widgets that will be used in this particular field of inputs"
        widget = wg.VBox((wg.HBox([wg.Label('Data Format Type: '),
                                wg.Dropdown(options=('Gridded', 'Tabular'))
                                ]),
            
                        wg.HBox([wg.Label("Compression Algorithm: "),
                                wg.SelectMultiple(options=('lz4',
                                                    'lz4hc',
                                                    'blosclz',
                                                    'zlib',
                                                    'zstd',
                                                    'gzip',
                                                    'bzip2'), value=('lz4',))
                                    ]),
                        wg.HBox([wg.Label("Compression Level: "),
                                wg.BoundedIntText(value=5,
                                                min=0,
                                                max=9,
                                                step=1, disabled=False)
                                ]),
                        wg.HBox([wg.Label("Chunksize (MB): "),
                                wg.FloatText(value='120')
                                ]),
                        wg.HBox([wg.Label("Datasets: "),
                                wg.SelectMultiple(options=self.gridded_datasets)
                                ])
                        ))
        return widget



    def display(self):
        "Identical code to the `.display()` method in `resourceWidgets`"
        box_list = [self.main]


        # Observer commands are more complicated than other classes. See following
        # comments for more information
        def observer_cmds():
            data = self.main.children

            for i in data:
                
                children = i.children
                format_type = children[0].children[1].value
                compression_algorithm = children[1].children[1]
                compression_level = children[2].children[1]
                datasets = children[4].children[1]

                if format_type == 'Tabular':
                    compression_algorithm.options=('snappy',
                                                    'lz4',
                                                    'gzip',
                                                    'zstd')
                    datasets.options = self.tabular_datasets
                elif format_type == 'Gridded':
                    compression_algorithm.options=('lz4',
                                                    'lz4hc',
                                                    'blosclz',
                                                    'zlib',
                                                    'zstd',
                                                    'gzip',
                                                    'bzip2')
                    datasets.options = self.gridded_datasets
        # END INNER FUNCTION

        def submit_cmds():
            boxes = self.main.children

            for n in boxes:
                for i in n.children:
                    if len(str(i.children[1].value)) == 0:
                        print('Ensure no fields are blank before submitting.')
                        return False
            
            return True
        # END INNER FUNCTION

        btn_box = self.AppendBoxes(box_title='Cloud-Native Data Option Set')
        box_list.append(btn_box)
        display(self.main)
        submit_btn = self.submit_button(observer_cmds = observer_cmds, submit_cmds = submit_cmds)
        box_list.append(submit_btn)
        [v.close() for v in box_list]



    def processInput(self):
        "New inputs can be defined and added on as the workflow develops"
        convert_options = []
        data = self.main.children

        for i in data:
            children = i.children

            # Populate dictionary with fields
            convert_options.append({"Algorithms" : children[1].children[1].value,
                            "Level" : children[2].children[1].value,
                            "Chunksize" : children[3].children[1].value,
                            "Datasets" : children[4].children[1].value})

        print('\n---------------------------------------------------------------------------')
        print('If you wish to change information about your conversion options, run this cell again.\n')
        return convert_options