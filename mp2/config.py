
HOSTS = [
    'fa23-cs425-2901.cs.illinois.edu',
    'fa23-cs425-2902.cs.illinois.edu',
    'fa23-cs425-2903.cs.illinois.edu',
    'fa23-cs425-2904.cs.illinois.edu',
    'fa23-cs425-2905.cs.illinois.edu',
    'fa23-cs425-2906.cs.illinois.edu',
    'fa23-cs425-2907.cs.illinois.edu',
    'fa23-cs425-2908.cs.illinois.edu',
    'fa23-cs425-2909.cs.illinois.edu',
    'fa23-cs425-2910.cs.illinois.edu'
]

NEIGHBORS = {
    'fa23-cs425-2901.cs.illinois.edu': [
        'fa23-cs425-2909.cs.illinois.edu',
        'fa23-cs425-2910.cs.illinois.edu',
        'fa23-cs425-2902.cs.illinois.edu',
        'fa23-cs425-2903.cs.illinois.edu',
    ],

    'fa23-cs425-2902.cs.illinois.edu': [
        'fa23-cs425-2910.cs.illinois.edu',
        'fa23-cs425-2901.cs.illinois.edu',
        'fa23-cs425-2903.cs.illinois.edu',
        'fa23-cs425-2904.cs.illinois.edu',
    ],

    'fa23-cs425-2903.cs.illinois.edu': [
        'fa23-cs425-2901.cs.illinois.edu',
        'fa23-cs425-2902.cs.illinois.edu',
        'fa23-cs425-2904.cs.illinois.edu',
        'fa23-cs425-2905.cs.illinois.edu',
    ],

    'fa23-cs425-2904.cs.illinois.edu': [
        'fa23-cs425-2902.cs.illinois.edu',
        'fa23-cs425-2903.cs.illinois.edu',
        'fa23-cs425-2905.cs.illinois.edu',
        'fa23-cs425-2906.cs.illinois.edu',
    ],

    'fa23-cs425-2905.cs.illinois.edu': [
        'fa23-cs425-2903.cs.illinois.edu',
        'fa23-cs425-2904.cs.illinois.edu',
        'fa23-cs425-2906.cs.illinois.edu',
        'fa23-cs425-2907.cs.illinois.edu',
    ],

    'fa23-cs425-2906.cs.illinois.edu': [
        'fa23-cs425-2904.cs.illinois.edu',
        'fa23-cs425-2905.cs.illinois.edu',
        'fa23-cs425-2907.cs.illinois.edu',
        'fa23-cs425-2908.cs.illinois.edu',
    ],    

    'fa23-cs425-2907.cs.illinois.edu': [
        'fa23-cs425-2905.cs.illinois.edu',
        'fa23-cs425-2906.cs.illinois.edu',
        'fa23-cs425-2908.cs.illinois.edu',
        'fa23-cs425-2909.cs.illinois.edu',
    ],

    'fa23-cs425-2908.cs.illinois.edu': [
        'fa23-cs425-2906.cs.illinois.edu',
        'fa23-cs425-2907.cs.illinois.edu',
        'fa23-cs425-2909.cs.illinois.edu',
        'fa23-cs425-2910.cs.illinois.edu',
    ],

    'fa23-cs425-2909.cs.illinois.edu': [
        'fa23-cs425-2907.cs.illinois.edu',
        'fa23-cs425-2908.cs.illinois.edu',
        'fa23-cs425-2910.cs.illinois.edu',
        'fa23-cs425-2901.cs.illinois.edu',
    ],    

    'fa23-cs425-2910.cs.illinois.edu': [
        'fa23-cs425-2908.cs.illinois.edu',
        'fa23-cs425-2909.cs.illinois.edu',
        'fa23-cs425-2901.cs.illinois.edu',
        'fa23-cs425-2902.cs.illinois.edu',
    ],

}

INTRODUCER = 'fa23-cs425-2903.cs.illinois.edu'
