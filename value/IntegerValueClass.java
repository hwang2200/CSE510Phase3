class IntegerValueClass extends ValueClass
{
	private int value;

	public IntegerValueClass()
	{
	
	}

	public IntegerValueClass(int value)
	{
		this.value = value;
	}

	public Integer getValue()
	{
		return value;
	}

	public void setValue(Integer value)
	{
		this.value = value;
	}
}
