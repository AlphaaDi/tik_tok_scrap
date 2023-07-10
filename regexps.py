import re

patterns = {
    'email': r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
    'twitter': r"@([A-Za-z0-9_]{1,15})",
    'instagram': r"@([A-Za-z0-9_](?:(?:[A-Za-z0-9_]|(?:\.(?!\.))){0,28}(?:[A-Za-z0-9_]))?)",
    'telegram': r"@([A-Za-z0-9_]{5,32})",
    'facebook': r"(https?://)?(www\.)?facebook\.com/[A-Za-z0-9_\-\.]+"
}

def find_all_websites(string):
    matches = []
    results = []
    for site, pattern in patterns.items():
        for match in re.finditer(pattern, string):
            start = match.start()
            end = match.end()
            start_context = max(0, start - 20)
            if not all(start >= m[1] or end <= m[0] for m in matches):
                continue
            matches.append((start, end))
            context = string[start_context:end]
            results.append(context)

    return results


def get_social_network(string, web_name='email'):
    match = re.search(patterns[web_name], string)
    if match:
        context = string[match.start():match.end()]
        return context
    return ""