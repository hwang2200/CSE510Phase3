package value;

public class StringValueClass extends ValueClass
{
	private String value;
	
	public StringValueClass()
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
