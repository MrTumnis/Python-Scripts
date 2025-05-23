import numpy as np
import scipy.optimize

# Given data points
voltage = np.array([1.00, 1.93, 2.87, 3.70, 4.41, 5.00])
flow_rate = np.array([0, 4, 8, 12, 16, 20])
# first = np.array([0.13, 0.19, 13.6, 13.63, 26.8, 26.84])
# second = np.array([0.4, 0.4, 13.85, 13.83, 27.16, 27.1])

# Fit a quadratic function: y = ax^2 + bx + c
quadratic_coeffs = np.polyfit(first, second, 2)

# Fit a cubic function: y = ax^3 + bx^2 + cx + d
cubic_coeffs = np.polyfit(first, second, 3)

# Return the coefficients for both fits
print("Quadratic:", quadratic_coeffs, "Cubic:", cubic_coeffs)
