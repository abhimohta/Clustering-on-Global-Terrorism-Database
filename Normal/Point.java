package abc;

import java.text.DecimalFormat;
import java.util.*;

/*
 * This is a point class, and it can be any dimention as you defined
 */
public class Point 
{
	// define the dimention of the point
	public static final int DIMENTION = 2;
	// store values for different dimention in array
	public int arr[];
	 
	public Point()
	{
		arr = new int[DIMENTION];
		for( int i = 0;i < DIMENTION; ++i)
		{
			arr[i]=0;
		}
	}
	 
	// get Euler Distance
	public static double getEulerDist(Point vec1,Point vec2)
	{
		if(!(vec1.arr.length == DIMENTION && vec2.arr.length == DIMENTION))
		{
			System.exit(1);
		}
		double dist=0.0;
		for(int i=0;i<DIMENTION;++i)
		{
			dist+=(vec1.arr[i]-vec2.arr[i])*(vec1.arr[i]-vec2.arr[i]);
		}
		return Math.sqrt(dist);
	}

	// get Manhant distance between two point
	public static double getManhtDist(Point vec1,Point vec2)
	{
		if(!(vec1.arr.length==DIMENTION && vec2.arr.length==DIMENTION))
		{
			System.exit(1);
		}
		double dist=0.0;
		for(int i=0;i<DIMENTION;++i)
		{
			dist += Math.abs(vec1.arr[i]-vec2.arr[i]);
		}
		return dist;
	}
	
	// do clear up 
	public void clear()
	{
		for(int i = 0;i < arr.length;i++)
		{
			arr[i] = 0;
		}
	}
	
	public String toString()
	{
		String rect = String.valueOf(arr[0]);
		for(int i = 1;i < DIMENTION; i++)
		{
			rect += "," + String.valueOf(arr[i]);
		}
		return rect;
	}
 
}
