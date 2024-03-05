import value.ValueClass;

class StringValueClass extends ValueClass
{
	private String value;
	
	public StringValue()
	{

	}
	
	public StringValueClass(String value)
	{
		this.value = value;
	}

	public String getValue()
	{
		return value;
	}

	public void setValue(String value)
	{
		this.value = value;
	}
}
