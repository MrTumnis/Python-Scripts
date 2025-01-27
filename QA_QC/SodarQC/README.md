SODAR QA
If raw data has a validity code of 0 it is invalid. If it has a validity code of 9 it is valid.

[Perform Component Speed Profile Check]
This check compares the change in component speed between adjacent levels (range gates
against the user-specified maximum change allowed for the W (vertical) and U/V (horizontal)
components. The default criteria suggested are 2 m/s for the W component and 5 m/s for the U
and V components, but the values used will depend to some extent on the Model VT-1
configuration. Reliability values for both levels involved are set to 2.

For example, if the absolute value of the 30m and 40m vertical wind speeds differ by more than
2, the validity code should be set to 2.

If the if the absolute value of the 30m and 40m horizontal wind speeds differ by more than 5, the
validity code should be set to 2

Perform Vector Speed Profile Check: This test compares the change in the vector speed
between adjacent levels (range gates) against the user-specified maximum change allowed. The
default criterion suggested is 5 m/s.

For example, if the absolute value of the 30m and 40m vector speeds differ by more than 5 m/s,
the validity code should be set to 2.

[Perform Component Standard Deviation Check: This check compares the wind speed]
standard deviation for each component against the user-specified maximum allowed for the W
(vertical) and U/V (horizontal) components.
The default criteria suggested are 1 m/s for the W component, 5 m/s for the U and V compo-
nents, and 5 for the U/V ratio.
If std dev of W is &gt; 1 m/s or if the std dev of U and W &gt; 5 m/s or if U/V ratio &gt; 5, an exception
has occurred.

When an exception is identified, the reliability value for the data is set to 2.
Perform Noise Check: This option checks for noise contamination by looking for intervals
when component intensity values increase with height and the vector speed decreases with
height. In most situations, this test should be performed only during the daytime hours when the
signal intensity can be expected to decrease with height. By default, this check looks only at data
between 10:00 and 17:00, but different start and stop hours can be specified.
When an exception is identified, the date/time, the height, and the component are recorded in the
scan log. The reliability value for the data is set to 2.

[Perform Echo Check] 
The classic signature of echo interference is the combination of
increasing signal amplitude and decreasing component speed with height. This applies especially
during the daytime hours.

When an exception is identified, the date/time and height are recorded in the scan log. The
reliability value for the data is set to 2.

[Perform Precipitation Check]
Oftentimes during heavy precipitation, the W component
recorded by the Model VT-1 will be a large negative value, suggesting that the sodar signal is
detecting the falling precipitation rather than the vertical wind.

Data contamination by precipitation effects can be detected by checking for intervals with low
vertical wind speed (W). The default criterion flags intervals with a W component of -3 m/s or
less.

The SODAR has a precip detector which invalidates data. Data should be scrubbed to see if
there are vertical wind speeds which are less than -3 m/s. If so, these should be flagged with a 3.
Perform SNR Check: This option checks for noise by comparing the mean SNR (signal-to-
noise ratio) measured for the averaging time for each height and each beam against a minimum
SNR ratio.

[[Not Implimented]]
When the measured SNR value is less than the defined threshold, the reliability value for the data
is set to 2. In general, this check should not be used to validate data. It is better to rely on the
other validation checks to identify and flag suspect data. The SNR check in the Data Manager
should generally only be used as a last resort.

