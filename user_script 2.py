import hashlib

def crack_password(hash_algorithm, hashed_password, candidate):
    """Attempt to crack a hashed password."""
    # Hash the candidate password using the specified algorithm
    hashed_candidate = hashlib.new(hash_algorithm, candidate.encode()).hexdigest()
    # Compare the hashed candidate with the provided hashed password
    if hashed_candidate == hashed_password:
        return candidate  # Password cracked successfully
    else:
        return None  # Password not cracked
