The modeldbg directory is used to test generated models in the debugger.  We have found it
convenient to add the model as a project such that the model can be conveniently debugged
as an Eclipse project.  This is the place to do this.

For example, imagine you wanted to hook up your model's EnvContext to a Spark Cluster loaded
with RDDs and were interested in testing the accessor mechanism of the custom env context 
object you have developed to this end.

You can use eclipse to walk into your spark client code to see if it is performing as expected.

We will have more to say about debugging models in the documentation to be delivered.
