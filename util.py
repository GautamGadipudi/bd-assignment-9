from sys import argv


def get_args(is_q4=False):
    argv_len = len(argv)
    member_file = title_file = principal_file = ''
    if argv_len == 4:
        member_file = argv[1]
        principal_file = argv[2]
        title_file = argv[3]
    elif argv_len == 3:
        member_file = argv[1]
        principal_file = argv[2]
        if not is_q4:
            title_file = input("Please enter titles file: ")
    elif argv_len == 2:
        member_file = argv[1]
        principal_file = input("Please enter principals file: ")
        if not is_q4:
            title_file = input("Please enter titles file: ")
    else:
        member_file = input("Please enter members file: ")
        principal_file = input("Please enter principals file: ")
        if not is_q4:
            title_file = input("Please enter titles file: ")
    if is_q4:
        return member_file, principal_file
    else:
        return member_file, principal_file, title_file