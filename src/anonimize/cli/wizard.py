#!/usr/bin/env python3
"""Interactive CLI wizard for anonimize.

This wizard guides users through anonymization with a friendly,
step-by-step interface. No need to remember commands - just answer
simple questions.

Usage:
    anonimize-wizard          # Start the interactive wizard
    anonimize --wizard        # Alternative entry point
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Try to import questionary for nice prompts
try:
    import questionary
    from questionary import Choice

    HAS_QUESTIONARY = True
except ImportError:
    HAS_QUESTIONARY = False

from anonimize import __version__, anonymize, detect_pii, preview


# ANSI colors for terminal output
class Colors:
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def print_header():
    """Print the welcome header."""
    print(f"""
{Colors.CYAN}{Colors.BOLD}
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   🤖  ANONIMIZE WIZARD  v{__version__:<10}                        ║
║                                                              ║
║   Your friendly guide to data anonymization                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
{Colors.RESET}
I'll help you anonymize your data safely. Just answer a few 
questions and I'll handle the rest!
""")


def print_success(message: str):
    """Print a success message."""
    print(f"{Colors.GREEN}✓{Colors.RESET} {message}")


def print_info(message: str):
    """Print an info message."""
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {message}")


def print_warning(message: str):
    """Print a warning message."""
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {message}")


def print_error(message: str):
    """Print an error message."""
    print(f"{Colors.RED}✗{Colors.RESET} {message}")


def ask_text(message: str, default: str = "") -> str:
    """Ask for text input."""
    if HAS_QUESTIONARY:
        return questionary.text(message, default=default).ask() or default
    else:
        print(f"\n{message}")
        if default:
            print(f"  [default: {default}]")
        result = input("> ").strip()
        return result if result else default


def ask_select(message: str, choices: List[str], default: Optional[str] = None) -> str:
    """Ask user to select from choices."""
    if HAS_QUESTIONARY:
        return questionary.select(message, choices=choices, default=default).ask()
    else:
        print(f"\n{message}")
        for i, choice in enumerate(choices, 1):
            marker = "*" if choice == default else " "
            print(f"  {marker}{i}. {choice}")
        while True:
            try:
                result = input("> ").strip()
                if result.isdigit():
                    idx = int(result) - 1
                    if 0 <= idx < len(choices):
                        return choices[idx]
                elif result in choices:
                    return result
                elif not result and default:
                    return default
                print("Please enter a valid option number")
            except (ValueError, IndexError):
                print("Please enter a valid option")


def ask_confirm(message: str, default: bool = True) -> bool:
    """Ask yes/no question."""
    if HAS_QUESTIONARY:
        return questionary.confirm(message, default=default).ask()
    else:
        default_str = "Y/n" if default else "y/N"
        print(f"\n{message} [{default_str}]")
        result = input("> ").strip().lower()
        if not result:
            return default
        return result in ("y", "yes", "true")


def ask_checkbox(message: str, choices: List[str]) -> List[str]:
    """Ask user to select multiple options."""
    if HAS_QUESTIONARY:
        return questionary.checkbox(message, choices=choices).ask() or []
    else:
        print(f"\n{message}")
        print("  (Enter numbers separated by commas, or 'all')")
        for i, choice in enumerate(choices, 1):
            print(f"  [{i}] {choice}")
        result = input("> ").strip().lower()
        if result == "all":
            return choices
        try:
            indices = [int(x.strip()) - 1 for x in result.split(",")]
            return [choices[i] for i in indices if 0 <= i < len(choices)]
        except (ValueError, IndexError):
            return []


def step_welcome():
    """Welcome step - explain what we're doing."""
    print_header()

    if ask_confirm("Ready to anonymize some data?", default=True):
        return True
    else:
        print_info("No problem! Run `anonimize-wizard` when you're ready.")
        return False


def step_select_source() -> Optional[Path]:
    """Step 1: Select data source."""
    print(f"\n{Colors.BOLD}Step 1: Select your data source{Colors.RESET}")
    print("-" * 40)

    source_type = ask_select(
        "What type of data do you want to anonymize?",
        choices=[
            "CSV file",
            "JSON file",
            "JSON Lines file (.jsonl)",
            "Python data (I'm writing code)",
            "Database (coming soon)",
        ],
    )

    if source_type == "Database (coming soon)":
        print_warning("Database wizard coming soon! Use the Python API for now.")
        return None

    if source_type == "Python data (I'm writing code)":
        print_info("Great! Check out our examples:")
        print("  from anonimize import anonymize_data")
        print("  result = anonymize_data(my_data)")
        return None

    # Get file path
    while True:
        file_path = ask_text(f"Enter the path to your {source_type.split()[0]} file:")

        if not file_path:
            print_error("Please enter a file path")
            continue

        path = Path(file_path).expanduser().resolve()

        if not path.exists():
            print_error(f"File not found: {path}")
            print_info("Make sure the path is correct and the file exists")
            if not ask_confirm("Try again?"):
                return None
            continue

        # Verify file type
        expected_ext = source_type.split()[0].lower()
        if path.suffix.lower().lstrip(".") != expected_ext:
            print_warning(f"File extension is {path.suffix}, expected .{expected_ext}")
            if not ask_confirm("Continue anyway?"):
                continue

        print_success(f"Found file: {path}")
        return path


def step_detect_pii(file_path: Path) -> Dict[str, Any]:
    """Step 2: Detect PII in the file."""
    print(f"\n{Colors.BOLD}Step 2: Detecting PII in your data{Colors.RESET}")
    print("-" * 40)

    print_info("Scanning for sensitive information...")

    try:
        detected = detect_pii(file_path)

        if not detected:
            print_warning("No PII automatically detected")
            print_info("This could mean:")
            print("  - Your data doesn't contain standard PII patterns")
            print("  - Column names don't match known PII types")
            print("  - You may need to manually specify fields")
            return {}

        print_success(f"Found {len(detected)} potential PII fields:")
        print()

        # Display detected fields in a table
        print(f"  {'Column':<20} {'Type':<15} {'Confidence'}")
        print(f"  {'-'*20} {'-'*15} {'-'*10}")
        for col, info in detected.items():
            pii_type = info.get("type", "unknown") if isinstance(info, dict) else info
            confidence = (
                info.get("confidence", "auto") if isinstance(info, dict) else "auto"
            )
            conf_str = (
                f"{confidence:.0%}"
                if isinstance(confidence, float)
                else str(confidence)
            )
            print(f"  {col:<20} {pii_type:<15} {conf_str}")

        print()
        return detected

    except Exception as e:
        print_error(f"Error detecting PII: {e}")
        return {}


def step_configure_columns(detected: Dict[str, Any], file_path: Path) -> List[str]:
    """Step 3: Select which columns to anonymize."""
    print(f"\n{Colors.BOLD}Step 3: Configure anonymization{Colors.RESET}")
    print("-" * 40)

    if not detected:
        print_warning("No PII detected automatically")
        print_info("You'll need to manually specify columns (advanced)")
        return []

    columns = list(detected.keys())

    # Ask which columns to anonymize
    selected = ask_checkbox(
        "Which columns would you like to anonymize?", choices=columns
    )

    if not selected:
        print_warning("No columns selected")
        if ask_confirm("Select all detected columns instead?"):
            selected = columns
        else:
            return []

    print_success(f"Selected {len(selected)} columns for anonymization")
    return selected


def step_select_strategy() -> str:
    """Step 4: Select anonymization strategy."""
    print(f"\n{Colors.BOLD}Step 4: Choose anonymization strategy{Colors.RESET}")
    print("-" * 40)

    print_info("How should we anonymize the data?")
    print()

    strategy = ask_select(
        "Select strategy:",
        choices=[
            Choice("Replace with fake data (realistic, consistent)", value="replace"),
            Choice("Mask (show last 4 chars only: j***@example.com)", value="mask"),
            Choice("Hash (one-way, irreversible)", value="hash"),
            Choice("Remove (delete the data entirely)", value="remove"),
        ],
        default="replace",
    )

    strategy_map = {
        "Replace with fake data (realistic, consistent)": "replace",
        "Mask (show last 4 chars only: j***@example.com)": "mask",
        "Hash (one-way, irreversible)": "hash",
        "Remove (delete the data entirely)": "remove",
    }

    result = strategy_map.get(strategy, strategy)
    print_success(f"Using strategy: {result}")
    return result


def step_preview(file_path: Path, selected_columns: List[str], strategy: str):
    """Step 5: Preview changes."""
    print(f"\n{Colors.BOLD}Step 5: Preview the changes{Colors.RESET}")
    print("-" * 40)

    if not ask_confirm(
        "Would you like to preview the anonymization first?", default=True
    ):
        return True

    print_info("Here's how the first 3 rows will look:")
    print()

    try:
        preview_data = preview(file_path, num_rows=3, strategy=strategy)

        if preview_data:
            # Show preview in a simple format
            headers = list(preview_data[0].keys())
            print(f"  {' | '.join(headers[:4])}")  # Limit to first 4 columns
            print(f"  {'-' * 60}")
            for row in preview_data[:3]:
                values = [str(row.get(h, ""))[:15] for h in headers[:4]]
                print(f"  {' | '.join(values)}")

        print()

        if ask_confirm("Look good? Proceed with anonymization?", default=True):
            return True
        else:
            print_info("Let's adjust the settings...")
            return False

    except Exception as e:
        print_warning(f"Couldn't generate preview: {e}")
        return ask_confirm("Continue without preview?")


def step_execute(
    file_path: Path, selected_columns: List[str], strategy: str
) -> Optional[Path]:
    """Step 6: Execute anonymization."""
    print(f"\n{Colors.BOLD}Step 6: Running anonymization{Colors.RESET}")
    print("-" * 40)

    # Generate output path
    default_output = file_path.with_suffix(f".anonymized{file_path.suffix}")

    custom_output = ask_confirm(
        f"Save to default location ({default_output.name})?", default=True
    )

    if custom_output:
        output_path = default_output
    else:
        custom_path = ask_text("Enter output file path:", default=str(default_output))
        output_path = Path(custom_path)

    print_info("Anonymizing... this may take a moment for large files")
    print()

    try:
        result = anonymize(
            file_path,
            output_path,
            strategy=strategy,
            columns=selected_columns if selected_columns else None,
            progress=True,
        )

        print()
        print_success("Anonymization complete! 🎉")
        print()
        print(f"  Input:  {file_path}")
        print(f"  Output: {result}")

        if isinstance(result, str):
            output_size = Path(result).stat().st_size
            print(f"  Size:   {output_size:,} bytes")

        return Path(result) if isinstance(result, str) else None

    except Exception as e:
        print_error(f"Anonymization failed: {e}")
        print_info("Common issues:")
        print("  - Check that the input file isn't open in another program")
        print("  - Ensure you have write permissions for the output directory")
        print("  - For large files, ensure you have enough disk space")
        return None


def step_next_actions(output_path: Optional[Path]):
    """Final step: Suggest next actions."""
    print(f"\n{Colors.BOLD}What would you like to do next?{Colors.RESET}")
    print("-" * 40)

    actions = [
        "Exit wizard",
        "Anonymize another file",
    ]

    if output_path:
        actions.insert(1, "View the anonymized file")
        actions.insert(2, "Compare original vs anonymized")

    action = ask_select("Select an option:", choices=actions)

    if action == "Exit wizard":
        print()
        print(f"{Colors.GREEN}Thanks for using Anonimize!{Colors.RESET}")
        print("Questions? Visit https://github.com/rar-file/anonimize")
        return "exit"

    elif action == "Anonymize another file":
        return "restart"

    elif action == "View the anonymized file":
        print_info(f"Opening {output_path}...")
        # Could add file opening logic here
        return "exit"

    elif action == "Compare original vs anonymized":
        print_info("Comparison feature coming soon!")
        return "exit"

    return "exit"


def run_wizard():
    """Run the interactive wizard."""
    try:
        # Welcome
        if not step_welcome():
            return 0

        while True:
            # Step 1: Select source
            file_path = step_select_source()
            if file_path is None:
                if not ask_confirm("Start over?"):
                    break
                continue

            # Step 2: Detect PII
            detected = step_detect_pii(file_path)

            # Step 3: Configure columns
            selected_columns = step_configure_columns(detected, file_path)

            # Step 4: Select strategy
            strategy = step_select_strategy()

            # Step 5: Preview (with retry loop)
            while True:
                if step_preview(file_path, selected_columns, strategy):
                    break
                # Allow changing settings
                strategy = step_select_strategy()

            # Step 6: Execute
            output_path = step_execute(file_path, selected_columns, strategy)

            # Next actions
            next_action = step_next_actions(output_path)

            if next_action == "exit":
                break
            elif next_action == "restart":
                print("\n" + "=" * 60 + "\n")
                continue

        return 0

    except KeyboardInterrupt:
        print()
        print_warning("\nWizard interrupted. No changes were made.")
        return 130
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        print_info(
            "If this persists, please report the issue with the error details above."
        )
        return 1


def main():
    """Main entry point for the wizard."""
    # Check for --wizard flag in args
    if len(sys.argv) > 1 and sys.argv[1] in ("--wizard", "-w", "wizard"):
        sys.argv.pop(1)  # Remove the flag

    return run_wizard()


if __name__ == "__main__":
    sys.exit(main())
