class get_strings:
    # `first_field` generates the first input field
    # when called
    def first_field(self):
        # Text box description
        first_desc = self.desc + ' 1:'
        
        # Initializes the first input field
        # and creates a group for future fields
        field1 = widgets.Text(
            placeholder=self.plholdr,
            description=first_desc
            )
        self.fields_box = widgets.VBox([field1])
    
    # `add_fields` assigns input field generating
    # functionality to a button
    def add_fields(self, btn):
        # Determines how many fields are in the group
        current_children = self.fields_box.children
        
        # Sets description for next field
        length = len(current_children)
        next_desc = self.desc + ' ' + str(length+1) + ':'
        
        # Creates new field and adds it to the group
        next_field = widgets.Text(
            placeholder=self.plholdr,
            description=next_desc
            )
        self.fields_box.children = tuple(list(current_children) + [next_field])
    
    # `del_fields` assigns input field deleting
    # functionality to a button
    def del_fields(self, btn):
        # Determines how many fields are currently
        # in the group.
        current_children = self.fields_box.children
        
        # Only removes a field if there are more than one
        # (i.e., it will not delete a field if there is
        # only one left)
        if len(current_children) > 1:
            # Remove the most recently generated field
            # in the group.
            remove = current_children[-1]
            self.fields_box.children = current_children[:-1]
            remove.close()
     
    # `on_submit` assigns the user input to an output variable
    # and sets a submission flag to True
    def on_submit(self, btn):
        # Check to see if any input fields are empty before
        # authorizing submission
        self.data = tuple(v.value for v in self.fields_box.children)
        if any(len(ele) == 0 for ele in self.data):
            print('Ensure no fields are blank before submitting.')
        else:
            self.submit = True
    
    # `run_ui` assigns functionality to all widgets and
    # orchestrates the input submission process
    def run_ui(self, placeholder, description):
        # Recievies input from the function call for use
        # in text box field information
        self.plholdr = placeholder
        self.desc = description
        
        # Set submission flag to false before generating widgets
        self.submit = False
        
        # Generate buttons and place them in a group
        button_purpose = ('Add field', 'Remove field', 'Submit')
        buttons = tuple(widgets.Button(description=v) for v in button_purpose)
        button_box = widgets.Box(children=buttons)
        
        # Assign a function to each button when it is pressed
        buttons[0].on_click(self.add_fields)
        buttons[1].on_click(self.del_fields)
        buttons[2].on_click(self.on_submit)
        
        # Initialize the first field and display the buttons
        # and input fields to the screen
        self.first_field()
        display(button_box, self.fields_box)
        
        # Polls the UI events to see if the submission button
        # has been pressed. If not pressed, will suspend cell
        # execution until user presses `Submit`.
        with ui_events() as poll:
            while not self.submit:
                poll(10) # Poll 10 UI events at a time
                time.sleep(0.1) # Recheck every 0.1 seconds
                
        # After submission is recieved, close all widgets in
        # the UI, confirm that the input data has been recieved,
        # and return data to variable given at function call
        button_box.close()
        self.fields_box.close()
        return self.data
            
getStrings = get_strings()

class randgen_options:    
    # Creates all widgets for randomly generated
    # file options.
    def set_widgets(self):
        # Dropdown box for choice of which resource to use to write randomly
        # generated files to the cloud
        self.rand_resource = widgets.Dropdown(options=self.resources,
                                        description='Resource to write files with:')

        # Checkboxes that allow the user to select one or more file types to randomly generate
        checks = tuple(widgets.Checkbox(description=v) for v in self.filetypes)
        self.checks_box = widgets.HBox(children=checks)

        # Float-accpeting text boxes that allow user to set the size of the randomly generated files
        filesize = tuple(widgets.FloatText(placeholder='Enter size of ' + v + ' (in GB)') for v in self.filetypes)
        self.size_box = widgets.HBox(children=filesize)

        # Submission button to record user input and end UI
        # execution.
        submit_btn = widgets.Button(description='Submit')
        submit_btn.on_click(self.on_submit)

        # Place all widgets in a tuple and output back to `run_ui`
        order = (self.rand_resource, self.checks_box, self.size_box, submit_btn)
        return order
    
    # Keeps track of UI events to see when submission button is pressed.
    # Also tracks the state of widgets and enables/disables the file size
    # text boxes.
    def poll_ui(self):
        with ui_events() as poll:
            # Loop runs until submission button is pressed
            while not self.submit:
                poll(10) # Process 10 UI events at a time
                
                # Determine current state of check boxes and file size boxes
                check_status = tuple(v.value for v in self.checks_box.children)
                disable_status = tuple(v.disabled for v in self.size_box.children)
                
                # Loops through each file type. Disables the file size fields
                # if their corresponding check boxes are not checked. Otherwise,
                # activates the check boxes.
                for i in range(len(check_status)):
                    if check_status[i] == False & disable_status[i] == False:
                        self.size_box.children[i].disabled = True
                    else:
                        self.size_box.children[i].disabled = False
                time.sleep(0.1) # Run loop every 0.1 seconds
    
    def check_input(self):
        filetype_bool = [v.value for v in self.checks_box.children]
        filetype_desc = [v.description for v in self.checks_box.children]
        filesize = [v.value for v in self.size_box.children]
        true_index = [i for i, v in enumerate(filetype_bool) if v == True]
        for i in true_index:
            if not filesize[i]:
                print(filetype_desc[i], 'must have nonzero size.')
                self.cont = False
                break

    # Set submission flag to true
    # upon click of `Submit` button.
    # Call `check_input` to ensure
    # that all desired file formats
    # have a nonzero size.
    def on_submit(self, btn):
        self.cont = True
        self.check_input()
        if self.cont:
            self.submit = True

    # Upon submission, processes final user input based on which boxes are checked.
    # In addition, pulls the corresponding file size and records the desired resource
    # for file writing.
    def process_input(self):
        # Resource name collection
        rname = (self.rand_resource.value,)
        
        # Pull the state and description of the check boxes,
        # as well with all file sizes input. Determine which
        # check boxes are checked (true_index)
        filetype_bool = [v.value for v in self.checks_box.children]
        filetype_desc = [v.description for v in self.checks_box.children]
        filesize = [v.value for v in self.size_box.children]
        true_index = [i for i, v in enumerate(filetype_bool) if v == True]
        
        # Initialize empty variables and populate them with the user's
        # desired file types and sizes
        rand_sizes = []
        rand_files = []
        if true_index:
            for i in true_index:
                rand_files.append(filetype_desc[i])
                rand_sizes.append(filesize[i])
        else:
            rand_files.append('None')
            rand_sizes.append('None')
        # Return the desired resource for use in file writing
        # and file types and their corresponding sizes
        return (rname, tuple(rand_files), tuple(rand_sizes))
        
        
    def run_ui(self, resources):
        # Set available resources, submission flag to
        # false, and define file types available for
        # random generation.
        self.resources = resources
        self.submit = False
        self.filetypes = ('CSV', 'NetCDF4', 'Binary')
        
        # Return required widgets, display them, and 
        # execute UI. When submit button is pressed,
        # process user input and close UI
        wg_order = self.set_widgets()
        [display(v) for v in wg_order]
        self.poll_ui()
        data = self.process_input()
        [v.close() for v in wg_order]
        
        # Return user input to calling body
        return data
        
randOpts = randgen_options()