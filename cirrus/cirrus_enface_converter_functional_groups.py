import pydicom


def source_image_sequence(dataset, x):
    """
    Add a Source Image Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Source Image Sequence will be added.
        x (list): A list containing dictionaries representing the source image sequence data. The expected structure of the dictionaries is:
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080100"].value
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080102"].value
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080104"].value
                  x[0]["00082112"].value[0]["00081150"].value
                  x[0]["00082112"].value[0]["00081155"].value

    This function constructs a PurposeOfReferenceCodeSequence and a SourceImageSequence from the provided data and adds them to the specified DICOM dataset.

    Returns:
        None
    """
    purpose_of_reference_code_seq = pydicom.Sequence()
    purpose_of_reference_code_item = pydicom.Dataset()
    purpose_of_reference_code_item.CodeValue = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080100"].value
    )
    purpose_of_reference_code_item.CodingSchemeDesignator = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080102"].value
    )
    purpose_of_reference_code_item.CodeMeaning = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080104"].value
    )
    purpose_of_reference_code_seq.append(purpose_of_reference_code_item)

    source_image_seq = pydicom.Sequence()
    dataset.SourceImageSequence = source_image_seq
    source_image_item = pydicom.Dataset()
    source_image_item.ReferencedSOPClassUID = (
        x[0]["00082112"].value[0]["00081150"].value
    )
    source_image_item.ReferencedSOPInstanceUID = (
        x[0]["00082112"].value[0]["00081155"].value
    )
    source_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_code_seq

    source_image_seq.append(source_image_item)

    dataset.SourceImageSequence = source_image_seq


def derivation_algorithm_sequence(dataset, x):
    """
    Add a Derivation Algorithm Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Derivation Algorithm Sequence will be added.
        x (list): A list containing dictionaries representing the derivation algorithm sequence data. The expected structure of the dictionaries is:
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080100"].value
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080102"].value
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080104"].value
                  x[0]["00221612"].value[0]["00660036"].value
                  x[0]["00221612"].value[0]["00660031"].value

    This function constructs an AlgorithmFamilyCodeSequence and a DerivationAlgorithmSequence from the provided data and adds them to the specified DICOM dataset.

    Returns:
        None
    """
    algorithm_family_code_seq = pydicom.Sequence()
    algorithm_family_code_item = pydicom.Dataset()
    algorithm_family_code_item.CodeValue = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080100"].value
    )
    algorithm_family_code_item.CodingSchemeDesignator = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080102"].value
    )
    algorithm_family_code_item.CodeMeaning = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080104"].value
    )
    algorithm_family_code_seq.append(algorithm_family_code_item)

    derivation_algorithm_seq = pydicom.Sequence()

    derivation_algorithm_item = pydicom.Dataset()
    derivation_algorithm_item.AlgorithmName = (
        x[0]["00221612"].value[0]["00660036"].value
    )
    derivation_algorithm_item.AlgorithmVersion = (
        x[0]["00221612"].value[0]["00660031"].value
    )

    derivation_algorithm_item.AlgorithmFamilyCodeSequence = algorithm_family_code_seq
    derivation_algorithm_seq.append(derivation_algorithm_item)

    dataset.DerivationAlgorithmSequence = derivation_algorithm_seq


def enface_volume_descriptor_sequence(dataset, x):
    """
    Add an En Face Volume Descriptor Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the sequence is added.
        x (dict): A dictionary containing the necessary values for the sequence fields.

            Expected keys:
            - 'EnFaceVolumeDescriptorScope': The scope of the En Face Volume descriptor.
            - 'ReferencedSOPClassUID': SOP Class UID of the referenced segmentation.
            - 'ReferencedSOPInstanceUID': SOP Instance UID of the referenced segmentation.
            - 'ReferencedSegmentNumber': Segment number of the referenced segmentation.
            - 'CodeValue': Code value for the segmented property type.
            - 'CodingSchemeDesignator': Coding scheme designator for the segmented property type.
            - 'CodeMeaning': Meaning of the segmented property type.
            - 'SurfaceOffset': Offset of the surface descriptor.

    Returns:
        None
    """

    enface_volume_descriptor_seq = pydicom.Sequence()

    # segmented_property_type_code_seq = pydicom.Sequence()
    # segmented_property_type_code_item = pydicom.Dataset()
    # segmented_property_type_code_item.CodeValue = ""
    # segmented_property_type_code_item.CodingSchemeDesignator = ""
    # segmented_property_type_code_item.CodeMeaning = ""
    # segmented_property_type_code_seq.append(segmented_property_type_code_item)

    # referenced_segmentation_seq = pydicom.Sequence()
    # referenced_segmentation_item = pydicom.Dataset()
    # referenced_segmentation_item.ReferencedSOPClassUID = ""
    # referenced_segmentation_item.ReferencedSOPInstanceUID = ""
    # referenced_segmentation_item.ReferencedSegmentNumber = ""
    # referenced_segmentation_item.SegmentedPropertyTypeCodeSequence = (
    #     segmented_property_type_code_seq
    # )
    # referenced_segmentation_seq.append(referenced_segmentation_item)

    # enface_volume_descriptor_item = pydicom.Dataset()
    # enface_volume_descriptor_item.EnFaceVolumeDescriptorScope = ""
    # enface_volume_descriptor_item.ReferencedSegmentationSequence = (
    #     referenced_segmentation_seq
    # )
    # enface_volume_descriptor_item.SurfaceOffset = ""

    # enface_volume_descriptor_seq.append(enface_volume_descriptor_item)

    dataset.EnFaceVolumeDescriptorSequence = enface_volume_descriptor_seq


def referenced_series_sequence(dataset, enface, seg, vol, opt, op):
    """
    Add a Referenced Series Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Referenced Series Sequence will be added.
        enface (list): A list of dictionaries representing enface data.
        seg (list): A list of dictionaries representing segmentation data.
        vol (list): A list of dictionaries representing volume data.
        opt (list): A list of dictionaries representing optical data.
        op (list): A list of dictionaries representing operational data.

    This function constructs a ReferencedSeriesSequence from the provided data and adds it to the specified DICOM dataset.
    It processes each of the provided lists (op, opt, vol, seg) and creates ReferencedInstanceSequence for each item in the lists.
    If the ReferencedSOPClassUID is "1.2.840.10008.5.1.4.1.1.66.5", it is replaced with "1.2.840.10008.5.1.4.xxxxx.1".

    Returns:
        Nones
    """

    list = [op, opt, vol, seg]

    referenced_series_seq = pydicom.Sequence()

    for i in list:
        referenced_instance_seq = pydicom.Sequence()

        referenced_instance_item = pydicom.Dataset()
        referenced_instance_item.ReferencedSOPClassUID = i[0]["00080016"].value
        referenced_instance_item.ReferencedSOPInstanceUID = i[0]["00080018"].value

        if i is seg:
            referenced_instance_item.ReferencedSOPClassUID = (
                "1.2.840.10008.5.1.4.xxxxx.1"
            )

        referenced_instance_seq.append(referenced_instance_item)

        referenced_series_item = pydicom.Dataset()
        referenced_series_item.SeriesInstanceUID = i[0]["0020000E"].value
        referenced_series_item.StudyInstanceUID = i[0]["0020000D"].value
        referenced_series_item.ReferencedInstanceSequence = referenced_instance_seq
        referenced_series_seq.append(referenced_series_item)

    dataset.ReferencedSeriesSequence = referenced_series_seq


def referenced_series_sequence_structural(dataset, enface, seg, opt, op):
    """
    Add a Referenced Series Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Referenced Series Sequence will be added.
        enface (list): A list of dictionaries representing enface data.
        seg (list): A list of dictionaries representing segmentation data.
        opt (list): A list of dictionaries representing optical data.
        op (list): A list of dictionaries representing operational data.

    This function constructs a ReferencedSeriesSequence from the provided data and adds it to the specified DICOM dataset.
    It processes each of the provided lists (op, opt, vol, seg) and creates ReferencedInstanceSequence for each item in the lists.
    If the ReferencedSOPClassUID is "1.2.840.10008.5.1.4.1.1.66.5", it is replaced with "1.2.840.10008.5.1.4.xxxxx.1".

    Returns:
        None
    """

    list = [op, opt, seg]

    referenced_series_seq = pydicom.Sequence()

    for i in list:
        referenced_instance_seq = pydicom.Sequence()

        referenced_instance_item = pydicom.Dataset()
        referenced_instance_item.ReferencedSOPClassUID = i[0]["00080016"].value
        referenced_instance_item.ReferencedSOPInstanceUID = i[0]["00080018"].value

        if i is seg:
            referenced_instance_item.ReferencedSOPClassUID = (
                "1.2.840.10008.5.1.4.xxxxx.1"
            )

        referenced_instance_seq.append(referenced_instance_item)

        referenced_series_item = pydicom.Dataset()
        referenced_series_item.SeriesInstanceUID = i[0]["0020000E"].value
        referenced_series_item.StudyInstanceUID = i[0]["0020000D"].value
        referenced_series_item.ReferencedInstanceSequence = referenced_instance_seq
        referenced_series_seq.append(referenced_series_item)

    dataset.ReferencedSeriesSequence = referenced_series_seq


def ophthalmic_image_type_code_sequence(dataset, x):
    """
    Add an Ophthalmic Image Type Code Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Ophthalmic Image Type Code Sequence will be added.
        x (list): A list containing dictionaries representing the ophthalmic image type data. The expected structure is:
                  x[0]["00221615"].value[0]["00080104"].value[0]

    This function maps the ophthalmic image type from the provided data to a corresponding code value and adds the code
    to an Ophthalmic Image Type Code Sequence in the specified DICOM dataset.

    The mapping of code values is as follows:
        - "Superficial Retina Vasculature Flow" -> "128265"
        - "Deep retina vasculature flow" -> "128269"
        - "Choriocapillaris vasculature flow" -> "128273"
        - "Outer retina vasculature flow" -> "128271"

    If the provided image type is found in the mapping, the corresponding code value is added to the sequence.
    The CodingSchemeDesignator is set to "DCM" and the CodeMeaning is set to the provided image type with proper capitalization.

    Returns:
        None
    """
    ophthalmic_image_type_code_seq = pydicom.Sequence()
    ophthalmic_image_type_code_item = pydicom.Dataset()

    ophthalmic_image_type_code_item.CodeValue = (
        x[0]["00221615"].value[0]["00080100"].value
    )
    ophthalmic_image_type_code_item.CodingSchemeDesignator = "DCM"
    ophthalmic_image_type_code_item.CodeMeaning = (
        x[0]["00221615"].value[0]["00080104"].value[0].lower().capitalize()
    )
    ophthalmic_image_type_code_seq.append(ophthalmic_image_type_code_item)
    dataset.OphthalmicImageTypeCodeSequence = ophthalmic_image_type_code_seq


def get_reference_coordinates(oct_file):
    """
    Extract reference coordinates from an OCT (Optical Coherence Tomography) DICOM file.

    This function reads a DICOM file, extracts 3D coordinates from the 'Ophthalmic Volumetric Properties'
    (tag '52009230') element, and calculates the minimum and maximum values for each coordinate component (x, y).
    These min/max values are used to determine the bounding box of the coordinates in a 2D plane.

    Args:
        oct_file (str): The path to the OCT DICOM file.

    Returns:
        list: A list of 4 values representing the bounding box coordinates:
              [x_min, y_max, x_max, y_min] in a 2D plane.

    The values represent:
        - x_min: The minimum value of the x-coordinates.
        - y_max: The maximum value of the y-coordinates.
        - x_max: The maximum value of the x-coordinates.
        - y_min: The minimum value of the y-coordinates.

    Raises:
        FileNotFoundError: If the specified DICOM file does not exist.
        KeyError: If the required DICOM tags are missing or improperly formatted.
    """

    list_coordinates = []
    a = pydicom.dcmread(oct_file)

    for i in range(len(a["52009230"].value)):
        x = a["52009230"].value[i]["00220031"].value[0]["00220032"].value
        list_coordinates.append(x)

    min_max_per_component = []
    num_components = len(list_coordinates[0])

    for n in range(num_components):
        # Extract the nth component from each sublist
        nth_components = [coords[n] for coords in list_coordinates]

        # Get the min and max of the nth components
        min_value = min(nth_components)
        max_value = max(nth_components)

        # Store the min and max for the nth component
        min_max_per_component.append((min_value, max_value))

    x_min = min(
        min_max_per_component[0][0],
        min_max_per_component[0][1],
        min_max_per_component[2][0],
        min_max_per_component[2][1],
    )
    y_max = max(
        min_max_per_component[1][0],
        min_max_per_component[1][1],
        min_max_per_component[3][0],
        min_max_per_component[3][1],
    )
    x_max = max(
        min_max_per_component[0][0],
        min_max_per_component[0][1],
        min_max_per_component[2][0],
        min_max_per_component[2][1],
    )
    y_min = min(
        min_max_per_component[1][0],
        min_max_per_component[1][1],
        min_max_per_component[3][0],
        min_max_per_component[3][1],
    )
    final_coordinates = [x_min, y_max, x_max, y_min]
    return final_coordinates


def ophthalmic_frame_location_sequence(dataset, x, opt, op):
    """
    Add an Ophthalmic Frame Location Sequence to a DICOM dataset.

    This function reads an optical coherence tomography (OCT) DICOM file and extracts the reference coordinates
    using the `get_reference_coordinates()` function. It then creates an Ophthalmic Frame Location Sequence
    with relevant image type code details and appends it to the provided DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The target DICOM dataset to which the Ophthalmic Frame Location Sequence
                                   will be added.
        x (dict): A dictionary of additional parameters (currently unused, but reserved for future customization).
        opt (str): The file path to the OCT DICOM file from which to extract reference coordinates
                   and SOP (Service Object Pair) UIDs.

    Returns:
        None: The function modifies the input dataset in place, adding the Ophthalmic Frame Location Sequence.

    The Ophthalmic Frame Location Sequence contains:
        - ReferencedSOPClassUID: SOP Class UID of the OCT DICOM file.
        - ReferencedSOPInstanceUID: SOP Instance UID of the OCT DICOM file.
        - ReferenceCoordinates: Extracted reference coordinates [x_min, y_max, x_max, y_min].

    Raises:
        FileNotFoundError: If the OCT DICOM file path is invalid.
        KeyError: If the required DICOM tags are missing or improperly formatted in the input file.
    """
    coordinates = get_reference_coordinates(opt)
    a = pydicom.dcmread(opt)
    b = pydicom.dcmread(op)
    ophthalmic_image_type_code_seq = pydicom.Sequence()
    ophthalmic_image_type_code_item = pydicom.Dataset()

    ophthalmic_image_type_code_item.ReferencedSOPClassUID = b.SOPClassUID
    ophthalmic_image_type_code_item.ReferencedSOPInstanceUID = b.SOPInstanceUID
    ophthalmic_image_type_code_item.ReferenceCoordinates = coordinates
    ophthalmic_image_type_code_seq.append(ophthalmic_image_type_code_item)
    dataset.OphthalmicFrameLocationSequence = ophthalmic_image_type_code_seq
