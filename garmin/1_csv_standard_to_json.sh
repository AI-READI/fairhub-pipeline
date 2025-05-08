### Usage Instructions:
### Please update the paths as needed

FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UAB/FitnessTracker/" #(it uses Garmin)
#FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UCSD_All/FitnessTracker" #(it uses GARMIN)
#FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UW/FitnessTracker" #(it uses GARMIN)

echo "Standardizing heart rate..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_heart_rate.py "$FitnessTracker_Path"
echo "Standardizing Oxygen saturation..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_oxygen_saturation.py "$FitnessTracker_Path"
echo "Standardizing Physical activities (Steps)..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_physical_activities.py "$FitnessTracker_Path"
echo "Standardizing Calories..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_physical_activity_calorie.py "$FitnessTracker_Path"
echo "Standardizing Respiratory rate..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_respiratory_rate.py "$FitnessTracker_Path"
echo "Standardizing Sleep stages..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_sleep_stages.py "$FitnessTracker_Path"
echo "Standardizing Stress..."
python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/standard_stress.py "$FitnessTracker_Path"





