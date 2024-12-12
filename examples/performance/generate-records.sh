#!/bin/bash

output_file="users.jsonl"
batch_size=100000
total_records=200000000

first_names=("James" "Mary" "John" "Patricia" "Robert" "Jennifer" "Michael" "Linda" "William" "Elizabeth" "David" "Susan" "Richard" "Jessica" "Joseph" "Sarah" "Thomas" "Karen" "Charles" "Nancy" "Christopher" "Margaret" "Daniel" "Lisa" "Matthew" "Betty" "Anthony" "Dorothy" "Donald" "Sandra" "Mark" "Ashley" "Paul" "Kimberly" "Steven" "Donna" "Andrew" "Emily" "Kenneth" "Michelle" "Joshua" "Carol" "Kevin" "Amanda" "Brian" "Melissa" "George" "Deborah" "Edward" "Stephanie" "Ronald" "Rebecca")

last_names=("Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis" "Rodriguez" "Martinez" "Hernandez" "Lopez" "Gonzalez" "Wilson" "Anderson" "Thomas" "Taylor" "Moore" "Jackson" "Martin" "Lee" "Perez" "Thompson" "White" "Harris" "Sanchez" "Clark" "Ramirez" "Lewis" "Robinson" "Walker" "Young" "Allen" "King" "Wright" "Scott" "Torres" "Nguyen" "Hill" "Flores" "Green" "Adams" "Nelson" "Baker" "Hall" "Rivera" "Campbell" "Mitchell" "Carter" "Roberts" "Gomez")

statuses=("active" "inactive")

# Clear the output file
> "$output_file"

for ((i=1; i<=total_records; i++)); do
    firstname=${first_names[$RANDOM % ${#first_names[@]}]}
    lastname=${last_names[$RANDOM % ${#last_names[@]}]}
    age=$((18 + RANDOM % 63))
    status=${statuses[$RANDOM % ${#statuses[@]}]}
    
    # Write each record directly to file with an explicit newline
    printf '%d;{"userId":%d,"firstname":"%s","lastname":"%s","age":%d,"status":"%s"}\n' \
        "$i" "$i" "$firstname" "$lastname" "$age" "$status" >> "$output_file"
    
    # Print progress every batch_size records
    if ((i % batch_size == 0)); then
        progress=$((i * 100 / total_records))
        echo "Progress: $progress% ($i records processed)"
    fi
done

echo "Done! Generated $total_records records in $output_file"